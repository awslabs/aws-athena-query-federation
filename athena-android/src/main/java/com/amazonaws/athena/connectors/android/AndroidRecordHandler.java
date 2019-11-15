/*-
 * #%L
 * athena-android
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.android;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
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

    private final AndroidDeviceTable androidTable = new AndroidDeviceTable();
    private final ObjectMapper mapper = new ObjectMapper();
    private final AmazonSQS amazonSQS;
    private final LiveQueryService liveQueryService;
    private final String queueUrl;

    public AndroidRecordHandler()
    {
        this(AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                AmazonSQSClientBuilder.defaultClient(),
                new LiveQueryService(System.getenv(FIREBASE_CONFIG), System.getenv(FIREBASE_DB_URL)));
    }

    @VisibleForTesting
    protected AndroidRecordHandler(AmazonS3 amazonS3,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            AmazonSQS amazonSQS,
            LiveQueryService liveQueryService)
    {
        super(amazonS3, secretsManager, athena, sourceType);
        this.amazonSQS = amazonSQS;
        this.liveQueryService = liveQueryService;
        GetQueueUrlResult queueUrlResult = amazonSQS.getQueueUrl(System.getenv(RESPONSE_QUEUE_NAME));
        queueUrl = queueUrlResult.getQueueUrl();
    }

    @Override
    protected void readWithConstraint(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest, QueryStatusChecker queryStatusChecker)
    {
        QueryRequest request = QueryRequest.newBuilder()
                .withQueryId(readRecordsRequest.getQueryId())
                .withQuery("query details")
                .withResponseQueue(queueUrl)
                .build();

        String response = liveQueryService.broadcastQuery(readRecordsRequest.getTableName().getTableName(), request);
        logger.info("readWithConstraint: Android broadcast result: " + response);

        readResultsFromSqs(blockSpiller, readRecordsRequest);
    }

    private void readResultsFromSqs(BlockSpiller blockSpiller, ReadRecordsRequest readRecordsRequest)
    {
        final Map<String, Field> fields = new HashMap<>();
        readRecordsRequest.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withWaitTimeSeconds(1);

        ValueSet queryTimeoutValueSet = readRecordsRequest.getConstraints().getSummary().get(androidTable.getQueryTimeout());
        ValueSet minResultsValueSet = readRecordsRequest.getConstraints().getSummary().get(androidTable.getQueryMinResultsField());

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

                                matches &= block.offerValue(androidTable.getDeviceIdField(), effectiveRow, queryResponse.getDeviceId());
                                matches &= block.offerValue(androidTable.getNameField(), effectiveRow, queryResponse.getName());
                                matches &= block.offerValue(androidTable.getEchoValueField(), effectiveRow, queryResponse.getEchoValue());
                                matches &= block.offerValue(androidTable.getLastUpdatedField(), effectiveRow, System.currentTimeMillis());
                                matches &= block.offerValue(androidTable.getResultField(), effectiveRow, nextVal);
                                matches &= block.offerValue(androidTable.getScoreField(), effectiveRow, queryResponse.getRandom());
                                matches &= block.offerValue(androidTable.getQueryMinResultsField(), effectiveRow, minResults);
                                matches &= block.offerValue(androidTable.getQueryTimeout(), effectiveRow, maxWaitTime);

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
