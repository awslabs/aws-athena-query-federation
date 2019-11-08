/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
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
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.google.common.base.Preconditions.checkArgument;

public class DynamoDBRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandler.class);
    private static final String sourceType = "ddb";

    private static final String HASH_KEY_VALUE_ALIAS = ":hashKeyValue";

    private static final TypeReference<HashMap<String, String>> STRING_MAP_TYPE_REFERENCE = new TypeReference<HashMap<String, String>>() {};
    private static final TypeReference<HashMap<String, AttributeValue>> ATTRIBUTE_VALUE_MAP_TYPE_REFERENCE = new TypeReference<HashMap<String, AttributeValue>>() {};

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
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest)
    {
        Split split = recordsRequest.getSplit();
        String tableName = recordsRequest.getTableName().getTableName();
        Iterator<Map<String, AttributeValue>> itemIterator = getIterator(split, tableName);

        long numRows = 0;
        AtomicLong numResultRows = new AtomicLong(0);
        while (itemIterator.hasNext()) {
            numRows++;
            spiller.writeRows((Block block, int rowNum) -> {
                Map<String, AttributeValue> item = itemIterator.next();
                if (item == null) {
                    // this can happen regardless of the hasNext() check above for the very first iteration since itemIterator
                    // had not made any DDB calls yet and there may be zero items returned when it does
                    return 0;
                }

                boolean matched = true;
                numResultRows.getAndIncrement();
                for (Field nextField : recordsRequest.getSchema().getFields()) {
                    Object value = ItemUtils.toSimpleValue(item.get(nextField.getName()));
                    Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                    try {
                        switch (fieldType) {
                            case LIST:
                                // DDB may return Set so coerce to List
                                List valueAsList = value != null ? new ArrayList((Collection) value) : null;
                                matched &= block.offerComplexValue(nextField.getName(),
                                        rowNum,
                                        FieldResolver.DEFAULT,
                                        valueAsList);
                                break;
                            case STRUCT:
                                matched &= block.offerComplexValue(nextField.getName(),
                                        rowNum,
                                        (Field field, Object val) -> ((Map) val).get(field.getName()), value);
                                break;
                            default:
                                matched &= block.offerValue(nextField.getName(), rowNum, value);
                                break;
                        }

                        if (!matched) {
                            return 0;
                        }
                    }
                    catch (Exception ex) {
                        throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                    }
                }
                return 1;
            });
        }

        logger.info("readWithConstraint: numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
    }

    private AmazonWebServiceRequest buildReadRequest(Split split, String tableName)
    {
        validateExpectedMetadata(split.getProperties());
        String rangeKeyFilter = split.getProperty(RANGE_KEY_FILTER_METADATA);
        String nonKeyFilter = split.getProperty(NON_KEY_FILTER_METADATA);
        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        if (rangeKeyFilter != null || nonKeyFilter != null) {
            try {
                expressionAttributeNames.putAll(Jackson.getObjectMapper().readValue(split.getProperty(EXPRESSION_NAMES_METADATA), STRING_MAP_TYPE_REFERENCE));
                expressionAttributeValues.putAll(Jackson.getObjectMapper().readValue(split.getProperty(EXPRESSION_VALUES_METADATA), ATTRIBUTE_VALUE_MAP_TYPE_REFERENCE));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        boolean isQuery = split.getProperty(SEGMENT_ID_PROPERTY) == null;

        if (isQuery) {
            String indexName = split.getProperty(INDEX_METADATA);
            String hashKeyName = split.getProperty(HASH_KEY_NAME_METADATA);
            String hashKeyAlias = DDBPredicateUtils.aliasColumn(hashKeyName);
            String keyConditionExpression = hashKeyAlias + " = " + HASH_KEY_VALUE_ALIAS;
            if (rangeKeyFilter != null) {
                keyConditionExpression += " AND " + rangeKeyFilter;
            }
            expressionAttributeNames.put(hashKeyAlias, hashKeyName);
            expressionAttributeValues.put(HASH_KEY_VALUE_ALIAS, Jackson.fromJsonString(split.getProperty(hashKeyName), AttributeValue.class));

            return new QueryRequest()
                    .withTableName(tableName)
                    .withIndexName(indexName)
                    .withKeyConditionExpression(keyConditionExpression)
                    .withFilterExpression(nonKeyFilter)
                    .withExpressionAttributeNames(expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues);
        }
        else {
            int segmentId = Integer.parseInt(split.getProperty(SEGMENT_ID_PROPERTY));
            int segmentCount = Integer.parseInt(split.getProperty(SEGMENT_COUNT_METADATA));

            return new ScanRequest()
                    .withTableName(tableName)
                    .withSegment(segmentId)
                    .withTotalSegments(segmentCount)
                    .withFilterExpression(nonKeyFilter)
                    .withExpressionAttributeNames(expressionAttributeNames.isEmpty() ? null : expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues.isEmpty() ? null : expressionAttributeValues);
        }
    }

    private Iterator<Map<String, AttributeValue>> getIterator(Split split, String tableName)
    {
        AmazonWebServiceRequest request = buildReadRequest(split, tableName);
        return new Iterator<Map<String, AttributeValue>>() {
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
                Iterator<Map<String, AttributeValue>> iterator;
                if (request instanceof QueryRequest) {
                    QueryRequest paginatedRequest = ((QueryRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Invoking DDB with Query request: {}", request);
                    }
                    QueryResult queryResult = ddbClient.query(paginatedRequest);
                    lastKeyEvaluated.set(queryResult.getLastEvaluatedKey());
                    iterator = queryResult.getItems().iterator();
                }
                else {
                    ScanRequest paginatedRequest = ((ScanRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Invoking DDB with Scan request: {}", request);
                    }
                    ScanResult scanResult = ddbClient.scan(paginatedRequest);
                    lastKeyEvaluated.set(scanResult.getLastEvaluatedKey());
                    iterator = scanResult.getItems().iterator();
                }
                currentPageIterator.set(iterator);
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                else {
                    return null;
                }
            }
        };
    }

    private void validateExpectedMetadata(Map<String, String> metadata)
    {
        boolean isQuery = !metadata.containsKey(SEGMENT_ID_PROPERTY);
        if (isQuery) {
            checkArgument(metadata.containsKey(HASH_KEY_NAME_METADATA), "Split missing expected metadata [%s]", HASH_KEY_NAME_METADATA);
        }
        else {
            checkArgument(metadata.containsKey(SEGMENT_COUNT_METADATA), "Split missing expected metadata [%s]", SEGMENT_COUNT_METADATA);
        }
        if (metadata.containsKey(RANGE_KEY_FILTER_METADATA) || metadata.containsKey(NON_KEY_FILTER_METADATA)) {
            checkArgument(metadata.containsKey(EXPRESSION_NAMES_METADATA), "Split missing expected metadata [%s] when filters are present", EXPRESSION_NAMES_METADATA);
            checkArgument(metadata.containsKey(EXPRESSION_VALUES_METADATA), "Split missing expected metadata [%s] when filters are present", EXPRESSION_VALUES_METADATA);
        }
    }
}
