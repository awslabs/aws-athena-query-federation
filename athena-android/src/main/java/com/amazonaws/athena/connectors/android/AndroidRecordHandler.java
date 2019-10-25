package com.amazonaws.athena.connectors.android;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AndroidRecordHandler
        extends RecordHandler
{
    private static final String sourceType = "android";
    private static final Logger logger = LoggerFactory.getLogger(AndroidRecordHandler.class);

    private static final String FIREBASE_DB_URL = "FIREBASE_DB_URL";
    private static final String FIREBASE_CONFIG = "FIREBASE_CONFIG";
    private static final String RESPONSE_QUEUE_NAME = "RESPONSE_QUEUE_NAME";
    private static final String MAX_WAIT_TIME = "MAX_WAIT_TIME";
    private static final String MIN_RESULTS = "MIN_RESULTS";

    private final AndroidDeviceTable androidDeviceTable = new AndroidDeviceTable();
    private final ObjectMapper mapper = new ObjectMapper();
    private final AmazonSQS amazonSQS;
    private final LiveQueryService liveQueryService;
    private final String queueUrl;

    public AndroidRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonSQSClientBuilder.defaultClient(),
                new LiveQueryService(System.getenv(FIREBASE_CONFIG), System.getenv(FIREBASE_DB_URL)));
    }

    @VisibleForTesting
    protected AndroidRecordHandler(AmazonS3 amazonS3,
            AWSSecretsManager secretsManager,
            AmazonSQS amazonSQS,
            LiveQueryService liveQueryService)
    {
        super(amazonS3, secretsManager, sourceType);
        this.amazonSQS = amazonSQS;
        this.liveQueryService = liveQueryService;
        GetQueueUrlResult queueUrlResult = amazonSQS.getQueueUrl(System.getenv(RESPONSE_QUEUE_NAME));
        queueUrl = queueUrlResult.getQueueUrl();
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
    {
        QueryRequest request = QueryRequest.newBuilder()
                .withQueryId(readRecordsRequest.getQueryId())
                .withQuery("query details")
                .withResponseQueue(queueUrl)
                .build();

        String response = liveQueryService.broadcastQuery(readRecordsRequest.getTableName().getTableName(), request);
        logger.info("readWithConstraint: Android broadcast result: " + response);

        readResultsFromSqs(constraintEvaluator, blockSpiller, readRecordsRequest);
    }

    private void readResultsFromSqs(ConstraintEvaluator constraintEvaluator, BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
    {
        final Map<String, Field> fields = new HashMap<>();
        readRecordsRequest.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(1);

        ValueSet queryTimeoutValueSet = readRecordsRequest.getConstraints().getSummary().get(androidDeviceTable.getQueryTimeout());
        ValueSet minResultsValueSet = readRecordsRequest.getConstraints().getSummary().get(androidDeviceTable.getQueryMinResultsField());

        long maxWaitTime = queryTimeoutValueSet != null && queryTimeoutValueSet.isSingleValue() ?
                (long) queryTimeoutValueSet.getSingleValue() : Long.parseLong(System.getenv(MAX_WAIT_TIME));
        long minResults = minResultsValueSet != null && minResultsValueSet.isSingleValue() ?
                (long) minResultsValueSet.getSingleValue() : Long.parseLong(System.getenv(MIN_RESULTS));

        logger.info("readResultsFromSqs: using timeout of " + maxWaitTime + " ms and min_results of " + minResults);

        long startTime = System.currentTimeMillis();
        long numResults = 0;
        ReceiveMessageResult receiveMessageResult;
        List<DeleteMessageBatchRequestEntry> msgsToAck = new ArrayList<>();
        do {
            receiveMessageResult = amazonSQS.receiveMessage(receiveRequest);
            for (com.amazonaws.services.sqs.model.Message next : receiveMessageResult.getMessages()) {
                try {
                    QueryResponse queryResponse = mapper.readValue(next.getBody(), QueryResponse.class);
                    if (queryResponse.getQueryId().equals(readRecordsRequest.getQueryId())) {
                        numResults++;
                        msgsToAck.add(new DeleteMessageBatchRequestEntry().withReceiptHandle(next.getReceiptHandle()).withId(next.getMessageId()));
                        blockSpiller.writeRows((Block block, int rowNum) -> {
                            int newRows = 0;

                            for (String nextVal : queryResponse.getValues()) {
                                boolean matches = true;
                                int effectiveRow = newRows + rowNum;

                                matches &= constraintEvaluator.apply(androidDeviceTable.getDeviceIdField(), queryResponse.getDeviceId());
                                if (matches && fields.containsKey(androidDeviceTable.getDeviceIdField())) {
                                    BlockUtils.setValue(androidDeviceTable.getDeviceIdField(block), effectiveRow, queryResponse.getDeviceId());
                                }

                                matches &= constraintEvaluator.apply(androidDeviceTable.getNameField(), queryResponse.getName());
                                if (matches && fields.containsKey(androidDeviceTable.getNameField())) {
                                    BlockUtils.setValue(androidDeviceTable.getNameField(block), effectiveRow, queryResponse.getName());
                                }

                                matches &= constraintEvaluator.apply(androidDeviceTable.getEchoValueField(), queryResponse.getEchoValue());
                                if (matches && fields.containsKey(androidDeviceTable.getEchoValueField())) {
                                    BlockUtils.setValue(androidDeviceTable.getEchoValueField(block), effectiveRow, queryResponse.getEchoValue());
                                }

                                if (matches && fields.containsKey(androidDeviceTable.getLastUpdatedField())) {
                                    BlockUtils.setValue(androidDeviceTable.getLastUpdatedField(block), effectiveRow, System.currentTimeMillis());
                                }

                                matches &= constraintEvaluator.apply(androidDeviceTable.getResultField(), nextVal);
                                if (matches && fields.containsKey(androidDeviceTable.getResultField())) {
                                    BlockUtils.setValue(androidDeviceTable.getResultField(block), effectiveRow, nextVal);
                                }

                                matches &= constraintEvaluator.apply(androidDeviceTable.getScoreField(), queryResponse.getRandom());
                                if (matches && fields.containsKey(androidDeviceTable.getScoreField())) {
                                    BlockUtils.setValue(androidDeviceTable.getScoreField(block), effectiveRow, queryResponse.getRandom());
                                }

                                if (matches && fields.containsKey(androidDeviceTable.getQueryMinResultsField())) {
                                    BlockUtils.setValue(androidDeviceTable.getQueryMinResultsField(block), effectiveRow, minResults);
                                }

                                if (matches && fields.containsKey(androidDeviceTable.getQueryTimeout())) {
                                    BlockUtils.setValue(androidDeviceTable.getQueryTimeout(block), effectiveRow, maxWaitTime);
                                }
                                newRows += matches ? 1 : 0;
                            }

                            return newRows;
                        });
                        logger.info("Received matching response " + queryResponse.toString());
                    }
                }
                catch (RuntimeException | IOException ex) {
                    logger.error("Error processing msg", ex);
                }
            }
            if (!msgsToAck.isEmpty()) {
                amazonSQS.deleteMessageBatch(queueUrl, msgsToAck);
                msgsToAck.clear();
            }
        }
        while (System.currentTimeMillis() - startTime < maxWaitTime && (numResults < minResults || receiveMessageResult.getMessages().size() > 0));
    }
}
