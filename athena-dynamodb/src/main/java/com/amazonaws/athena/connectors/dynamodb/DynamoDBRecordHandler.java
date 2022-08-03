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
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.throttling.DynamoDBExceptionFilter.EXCEPTION_FILTER;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Handles data read record requests for the Athena DynamoDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Reads and maps DynamoDB data for a specific split.  The split can either represent a single hash key
 * or a table scan segment.<br>
 * 2. Attempts to push down all predicates into DynamoDB to reduce read cost and bytes over the wire.
 */
public class DynamoDBRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordHandler.class);
    private static final String sourceType = "ddb";

    private static final String DISABLE_PROJECTION_AND_CASING_ENV = "disable_projection_and_casing";

    private static final String HASH_KEY_VALUE_ALIAS = ":hashKeyValue";

    private static final TypeReference<HashMap<String, String>> STRING_MAP_TYPE_REFERENCE = new TypeReference<HashMap<String, String>>() {};
    private static final TypeReference<HashMap<String, AttributeValue>> ATTRIBUTE_VALUE_MAP_TYPE_REFERENCE = new TypeReference<HashMap<String, AttributeValue>>() {};

    private final LoadingCache<String, ThrottlingInvoker> invokerCache = CacheBuilder.newBuilder().build(
        new CacheLoader<String, ThrottlingInvoker>() {
            @Override
            public ThrottlingInvoker load(String tableName)
                    throws Exception
            {
                return ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
            }
        });
    private final AmazonDynamoDB ddbClient;

    public DynamoDBRecordHandler()
    {
        super(sourceType);
        this.ddbClient = AmazonDynamoDBClientBuilder.standard().build();
    }

    @VisibleForTesting
    DynamoDBRecordHandler(AmazonDynamoDB ddbClient, AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, String sourceType)
    {
        super(amazonS3, secretsManager, athena, sourceType);
        this.ddbClient = ddbClient;
    }

    /**
     * Reads data from DynamoDB by submitting either a Query or a Scan, depending
     * on the type of split, and includes any filters specified in the split.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws ExecutionException
    {
        Split split = recordsRequest.getSplit();
        // use the property instead of the request table name because of case sensitivity
        String tableName = split.getProperty(TABLE_METADATA);
        invokerCache.get(tableName).setBlockSpiller(spiller);
        DDBRecordMetadata recordMetadata = new DDBRecordMetadata(recordsRequest.getSchema());

        String disableProjectionAndCasingEnvValue = System.getenv().getOrDefault(DISABLE_PROJECTION_AND_CASING_ENV, "auto").toLowerCase();
        logger.info(DISABLE_PROJECTION_AND_CASING_ENV + " environment variable set to: " + disableProjectionAndCasingEnvValue);

        boolean disableProjectionAndCasing = false;
        if (disableProjectionAndCasingEnvValue.equals("always")) {
            // This is when the user wants to turn this on unconditionally to
            // solve their casing issues even when they do not have `set` or
            // `decimal` columns.
            disableProjectionAndCasing = true;
        }
        else { // *** We default to auto when the variable is not set ***
            // In the automatic case, we will try to mimic the behavior prior to the support of `set` and `decimal` types as much
            // as possible.
            //
            // Previously, when the user used a Glue Table and had `set` and `decimal` types present, the code would have failed over
            // to using internal type inference.
            // Internal type inferencing uses the original column names from DDB since it is doing a partial scan of the DDB table and is
            // therefore able to read the fields with casing.
            //
            // To mimic this behavior at a similar cost, we will just disable projection and casing, where we don't need an additional partial
            // table scan to infer types, because we are using the glue types.
            // The only side effect of this is increased network bandwidth usage and latency increase (DDB read units remains the same).
            // If the DDB Connector and DDB Table are within the same region, this does not cost the user anything extra.
            // Additionally in regards to bandwidth and latency, in many cases, this will be a wash because we avoid doing a partial table scan
            // for type inference in this situation now.
            //
            // If the user is using `columnMapping`, then we will assume that they have correctly mapped their column names, and we will not
            // disable projection and casing.
            disableProjectionAndCasing = recordMetadata.getGlueTableContainedPreviouslyUnsupportedTypes() && recordMetadata.getColumnNameMapping().isEmpty();
            logger.info("GlueTableContainedPreviouslyUnsupportedTypes: " + recordMetadata.getGlueTableContainedPreviouslyUnsupportedTypes());
            logger.info("ColumnNameMapping isEmpty: " + recordMetadata.getColumnNameMapping().isEmpty());
            logger.info("Resolving disableProjectionAndCasing to: " + disableProjectionAndCasing);
        }

        Iterator<Map<String, AttributeValue>> itemIterator = getIterator(split, tableName, recordsRequest.getSchema(), disableProjectionAndCasing);
        DynamoDBFieldResolver resolver = new DynamoDBFieldResolver(recordMetadata);

        GeneratedRowWriter.RowWriterBuilder rowWriterBuilder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        //register extract and field writer factory for each field.
        for (Field next : recordsRequest.getSchema().getFields()) {
            Optional<Extractor> extractor = DDBTypeUtils.makeExtractor(next, recordMetadata, disableProjectionAndCasing);
            //generate extractor for supported data types
            if (extractor.isPresent()) {
                rowWriterBuilder.withExtractor(next.getName(), extractor.get());
            }
            else {
                //generate field writer factor for complex data types.
                rowWriterBuilder.withFieldWriterFactory(next.getName(), DDBTypeUtils.makeFactory(next, recordMetadata, resolver, disableProjectionAndCasing));
            }
        }

        GeneratedRowWriter rowWriter = rowWriterBuilder.build();
        long numRows = 0;

        while (itemIterator.hasNext()) {
            if (!queryStatusChecker.isQueryRunning()) {
                // we can stop processing because the query waiting for this data has already terminated
                return;
            }

            Map<String, AttributeValue> item = itemIterator.next();
            if (item == null) {
                // this can happen regardless of the hasNext() check above for the very first iteration since itemIterator
                // had not made any DDB calls yet and there may be zero items returned when it does
                continue;
            }
            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, item) ? 1 : 0);
            numRows++;
        }
        logger.info("readWithConstraint: numRows[{}]", numRows);
    }

    /*
    Converts a split into a Query or Scan request
     */
    private AmazonWebServiceRequest buildReadRequest(Split split, String tableName, Schema schema, boolean disableProjectionAndCasing)
    {
        validateExpectedMetadata(split.getProperties());
        // prepare filters
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

        // Only read columns that are needed in the query
        String projectionExpression = disableProjectionAndCasing ? null : schema.getFields()
                .stream()
                .map(field -> {
                    String aliasedName = DDBPredicateUtils.aliasColumn(field.getName());
                    expressionAttributeNames.put(aliasedName, field.getName());
                    return aliasedName;
                })
                .collect(Collectors.joining(","));

        boolean isQuery = split.getProperty(SEGMENT_ID_PROPERTY) == null;

        if (isQuery) {
            // prepare key condition expression
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
                    .withExpressionAttributeValues(expressionAttributeValues)
                    .withProjectionExpression(projectionExpression);
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
                    .withExpressionAttributeValues(expressionAttributeValues.isEmpty() ? null : expressionAttributeValues)
                    .withProjectionExpression(projectionExpression);
        }
    }

    /*
    Creates an iterator that can iterate through a Query or Scan, sending paginated requests as necessary
     */
    private Iterator<Map<String, AttributeValue>> getIterator(Split split, String tableName, Schema schema, boolean disableProjectionAndCasing)
    {
        AmazonWebServiceRequest request = buildReadRequest(split, tableName, schema, disableProjectionAndCasing);
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
                try {
                    if (request instanceof QueryRequest) {
                        QueryRequest paginatedRequest = ((QueryRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                        logger.info("Invoking DDB with Query request: {}", request);
                        QueryResult queryResult = invokerCache.get(tableName).invoke(() -> ddbClient.query(paginatedRequest));
                        lastKeyEvaluated.set(queryResult.getLastEvaluatedKey());
                        iterator = queryResult.getItems().iterator();
                    }
                    else {
                        ScanRequest paginatedRequest = ((ScanRequest) request).withExclusiveStartKey(lastKeyEvaluated.get());
                        logger.info("Invoking DDB with Scan request: {}", request);
                        ScanResult scanResult = invokerCache.get(tableName).invoke(() -> ddbClient.scan(paginatedRequest));
                        lastKeyEvaluated.set(scanResult.getLastEvaluatedKey());
                        iterator = scanResult.getItems().iterator();
                    }
                }
                catch (TimeoutException | ExecutionException e) {
                    throw new RuntimeException(e);
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

    /*
    Validates that the required metadata is present for split processing
     */
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
