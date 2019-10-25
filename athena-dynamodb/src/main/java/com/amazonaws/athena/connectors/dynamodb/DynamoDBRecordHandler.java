package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_ID_PROPERTY;

public class DynamoDBRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandler.class);
    private static final String sourceType = "ddb";

    private static final String HASH_KEY_ALIAS = "#hashKey";
    private static final String HASH_KEY_VALUE_ALIAS = ":hashKeyValue";

    private final AmazonDynamoDB ddbClient;

    public DynamoDBRecordHandler()
    {
        super(sourceType);
        this.ddbClient = AmazonDynamoDBClientBuilder.standard().build();
    }

    @VisibleForTesting
    DynamoDBRecordHandler(AmazonDynamoDB ddbClient, AmazonS3 amazonS3, AWSSecretsManager secretsManager, String sourceType)
    {
        super(amazonS3, secretsManager, sourceType);
        this.ddbClient = ddbClient;
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        Split split = recordsRequest.getSplit();
        String tableName = recordsRequest.getTableName().getTableName();
        Iterator<Map<String, AttributeValue>> itemIterator;
        boolean isQuery = split.getProperty(SEGMENT_ID_PROPERTY) == null;
        if (isQuery) {
            itemIterator = getQueryItemIterator(split, tableName);
        }
        else {
            itemIterator = getScanItemIterator(split, tableName);
        }

        long numRows = 0;
        AtomicLong numResultRows = new AtomicLong(0);
        while (itemIterator.hasNext()) {
            numRows++;
            spiller.writeRows((Block block, int rowNum) -> {
                Map<String, AttributeValue> item = itemIterator.next();

                boolean matched = true;
                for (Field nextField : recordsRequest.getSchema().getFields()) {
                    if (!matched) {
                        break;
                    }
                    matched = constraintEvaluator.apply(nextField.getName(), ItemUtils.toSimpleValue(item.get(nextField.getName())));
                }

                if (matched) {
                    numResultRows.getAndIncrement();
                    for (Field nextField : recordsRequest.getSchema().getFields()) {
                        FieldVector vector = block.getFieldVector(nextField.getName());
                        Object value = ItemUtils.toSimpleValue(item.get(nextField.getName()));
                        Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                        try {
                            switch (fieldType) {
                                case LIST:
                                    // DDB may return Set so coerce to List
                                    List valueAsList = new ArrayList((Collection) value);
                                    BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, valueAsList);
                                    break;
                                case STRUCT:
                                    BlockUtils.setComplexValue(vector, rowNum,
                                            (Field field, Object val) -> ((Map) val).get(field.getName()), value);
                                    break;
                                default:
                                    BlockUtils.setValue(vector, rowNum, value);
                                    break;
                            }
                        }
                        catch (Exception ex) {
                            throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                        }
                    }
                }

                return matched ? 1 : 0;
            });
        }

        logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
    }

    private Iterator<Map<String, AttributeValue>> getQueryItemIterator(Split split, String tableName)
    {
        String indexName = split.getProperty(INDEX_METADATA);
        String hashKeyName = split.getProperty(HASH_KEY_NAME_METADATA);
        // TODO add range key condition
        String keyConditionExpression = HASH_KEY_ALIAS + " = " + HASH_KEY_VALUE_ALIAS;
        Map<String, String> expressionAttributeNames = ImmutableMap.of(HASH_KEY_ALIAS, hashKeyName);
        Map<String, AttributeValue> expressionAttributeValues = ImmutableMap.of(HASH_KEY_VALUE_ALIAS, Jackson.fromJsonString(split.getProperty(hashKeyName), AttributeValue.class));
        return new Iterator<Map<String, AttributeValue>>()
        {
            AtomicReference<Map<String, AttributeValue>> lastKeyEvaluated = new AtomicReference<>();
            AtomicReference<Iterator<Map<String, AttributeValue>>> currentPageIterator = new AtomicReference<>();

            @Override
            public boolean hasNext()
            {
                return currentPageIterator.get() == null
                        || currentPageIterator.get().hasNext()
                        || lastKeyEvaluated.get() != null;
            }

            @Override
            public Map<String, AttributeValue> next()
            {
                if (currentPageIterator.get() != null && currentPageIterator.get().hasNext()) {
                    return currentPageIterator.get().next();
                }
                else {
                    QueryResult queryResult = ddbClient.query(new QueryRequest()
                            .withTableName(tableName)
                            .withIndexName(indexName)
                            .withKeyConditionExpression(keyConditionExpression)
                            .withExpressionAttributeNames(expressionAttributeNames)
                            .withExpressionAttributeValues(expressionAttributeValues)
                            // TODO add filter expression
                            //.withFilterExpression()
                            .withExclusiveStartKey(lastKeyEvaluated.get()));
                    lastKeyEvaluated.set(queryResult.getLastEvaluatedKey());
                    Iterator<Map<String, AttributeValue>> iterator = queryResult.getItems().iterator();
                    currentPageIterator.set(iterator);
                    if (iterator.hasNext()) {
                        return iterator.next();
                    }
                    else {
                        return ImmutableMap.of();
                    }
                }
            }
        };
    }

    private Iterator<Map<String, AttributeValue>> getScanItemIterator(Split split, String tableName)
    {
        int segmentId = Integer.parseInt(split.getProperty(SEGMENT_ID_PROPERTY));
        int segmentCount = Integer.parseInt(split.getProperty(SEGMENT_COUNT_METADATA));
        return new Iterator<Map<String, AttributeValue>>()
        {
            AtomicReference<Map<String, AttributeValue>> lastKeyEvaluated = new AtomicReference<>();
            AtomicReference<Iterator<Map<String, AttributeValue>>> currentPageIterator = new AtomicReference<>();

            @Override
            public boolean hasNext()
            {
                return currentPageIterator.get() == null
                        || currentPageIterator.get().hasNext()
                        || lastKeyEvaluated.get() != null;
            }

            @Override
            public Map<String, AttributeValue> next()
            {
                if (currentPageIterator.get() != null && currentPageIterator.get().hasNext()) {
                    return currentPageIterator.get().next();
                }
                else {
                    ScanResult scanResult = ddbClient.scan(new ScanRequest()
                            .withTableName(tableName)
                            .withSegment(segmentId)
                            .withTotalSegments(segmentCount)
                            // TODO add filter expression
                            //.withFilterExpression()
                            .withExclusiveStartKey(lastKeyEvaluated.get()));
                    lastKeyEvaluated.set(scanResult.getLastEvaluatedKey());
                    Iterator<Map<String, AttributeValue>> iterator = scanResult.getItems().iterator();
                    currentPageIterator.set(iterator);
                    if (iterator.hasNext()) {
                        return iterator.next();
                    }
                    else {
                        return ImmutableMap.of();
                    }
                }
            }
        };
    }
}
