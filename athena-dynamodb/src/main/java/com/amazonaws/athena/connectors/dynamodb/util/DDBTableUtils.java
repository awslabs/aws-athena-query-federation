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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * Provides utility methods relating to table handling.
 */
public final class DDBTableUtils
{
    private static final Logger logger = LoggerFactory.getLogger(DDBTableUtils.class);

    // for scan segmentation calculation
    private static final long PSUEDO_CAPACITY_FOR_ON_DEMAND = 40_000;
    private static final int MAX_SCAN_SEGMENTS = 1000000;
    private static final int MIN_SCAN_SEGMENTS = 1;
    private static final long MAX_BYTES_PER_SEGMENT = 1024L * 1024L * 1024L;
    private static final double MIN_IO_PER_SEGMENT = 100.0;
    private static final int SCHEMA_INFERENCE_NUM_RECORDS = 4;

    private DDBTableUtils() {}

    /**
     * Fetches metadata for a DynamoDB table
     *
     * @param tableName the (case sensitive) table name
     * @param invoker the ThrottlingInvoker to call DDB with
     * @param ddbClient the DDB client to use
     * @return the table metadata
     */
    public static DynamoDBTable getTable(String tableName, ThrottlingInvoker invoker, DynamoDbClient ddbClient)
            throws TimeoutException
    {
        DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();

        TableDescription table = invoker.invoke(() -> ddbClient.describeTable(request).table());

        KeyNames keys = getKeys(table.keySchema());

        // get data statistics
        long approxTableSizeInBytes = table.tableSizeBytes();
        long approxItemCount = table.itemCount();

        ProvisionedThroughputDescription provisionedThroughputDescription = table.provisionedThroughput();
        // #todo: from the Documentation; this doesn't seem to be returning null from the looks of it; but test;
        final long provisionedReadCapacity =  provisionedThroughputDescription != null ? provisionedThroughputDescription.readCapacityUnits() : PSUEDO_CAPACITY_FOR_ON_DEMAND;

        // get secondary indexes
        List<LocalSecondaryIndexDescription> localSecondaryIndexes = table.hasLocalSecondaryIndexes() ? table.localSecondaryIndexes() : ImmutableList.of();
        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = table.hasGlobalSecondaryIndexes() ? table.globalSecondaryIndexes() : ImmutableList.of();
        ImmutableList.Builder<DynamoDBIndex> indices = ImmutableList.builder();

        localSecondaryIndexes.forEach(i -> {
            KeyNames indexKeys = getKeys(i.keySchema());
            // DynamoDB automatically fetches all attributes from the table for local secondary index, so ignore projected attributes
            indices.add(new DynamoDBIndex(i.indexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), ProjectionType.ALL, ImmutableList.of()));
        });
        globalSecondaryIndexes.stream()
              .filter(i -> i.indexStatus().equals(IndexStatus.ACTIVE))
              .forEach(i -> {
                  KeyNames indexKeys = getKeys(i.keySchema());
                  indices.add(new DynamoDBIndex(i.indexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), i.projection().projectionType(),
                        i.projection().nonKeyAttributes() == null ? ImmutableList.of() : i.projection().nonKeyAttributes()));
              });

        return new DynamoDBTable(tableName, keys.getHashKey(), keys.getRangeKey(), table.attributeDefinitions(), indices.build(), approxTableSizeInBytes, approxItemCount, provisionedReadCapacity);
    }

    /*
    Parses the key attributes from the given list of KeySchemaElements
     */
    private static KeyNames getKeys(List<KeySchemaElement> keys)
    {
        String hashKey = null;
        String rangeKey = null;
        for (KeySchemaElement key : keys) {
            if (key.keyType().equals(KeyType.HASH)) {
                hashKey = key.attributeName();
            }
            else if (key.keyType().equals(KeyType.RANGE)) {
                rangeKey = key.attributeName();
            }
        }
        return new KeyNames(hashKey, rangeKey);
    }

    /**
     * Derives an Arrow {@link Schema} for the given table by performing a small table scan and mapping the returned
     * attribute values' types to Arrow types. If the table is empty, only attributes found in the table's metadata
     * are added to the return schema.
     *
     * @param tableName the table to derive a schema for
     * @param invoker the ThrottlingInvoker to call DDB with
     * @param ddbClient the DDB client to use
     * @return the table's derived schema
     */
    public static Schema peekTableForSchema(String tableName, ThrottlingInvoker invoker, DynamoDbClient ddbClient)
            throws TimeoutException
    {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .limit(SCHEMA_INFERENCE_NUM_RECORDS)
                .build();
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        try {
            ScanResponse scanResponse = invoker.invoke(() -> ddbClient.scan(scanRequest));
            if (!scanResponse.items().isEmpty()) {
                List<Map<String, AttributeValue>> items = scanResponse.items();
                Set<String> discoveredColumns = new HashSet<>();

                for (Map<String, AttributeValue> item : items) {
                    for (Map.Entry<String, AttributeValue> column : item.entrySet()) {
                        if (!discoveredColumns.contains(column.getKey())) {
                            Field field = DDBTypeUtils.inferArrowField(column.getKey(), column.getValue());
                            if (field != null) {
                                schemaBuilder.addField(field);
                                discoveredColumns.add(column.getKey());
                            }
                        }
                    }
                }
            }
            else {
                // there's no items, so use any attributes defined in the table metadata
                DynamoDBTable table = getTable(tableName, invoker, ddbClient);
                for (AttributeDefinition attributeDefinition : table.getKnownAttributeDefinitions()) {
                    schemaBuilder.addField(DDBTypeUtils.getArrowFieldFromDDBType(attributeDefinition.attributeName(), attributeDefinition.attributeType().toString()));
                }
            }
        }
        catch (RuntimeException runtimeException) {
            if (runtimeException.getMessage().contains("AWSKMSException")) {
                logger.warn("Failed to retrieve table schema due to KMS issue, empty schema for table: {}. Error Message: {}", tableName, runtimeException.getMessage());
            }
            else {
                throw runtimeException;
            }
        }
        return schemaBuilder.build();
    }

    /**
     * This hueristic determines an optimal segment count to perform Parallel Scans with using the table's capacity
     * and size.
     *
     * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan">
     *     https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan</a>
     * @param tableNormalizedReadThroughput the provisioned read capacity for the table
     * @param currentTableSizeBytes the table's approximate size in bytes
     * @return an optimal segment count
     */
    public static int getNumSegments(long tableNormalizedReadThroughput, long currentTableSizeBytes)
    {
        // Segments for size
        int numSegmentsForSize = (int) (currentTableSizeBytes / MAX_BYTES_PER_SEGMENT);
        logger.debug("Would use {} segments for size", numSegmentsForSize);

        // Segments for total throughput
        int numSegmentsForThroughput = (int) (tableNormalizedReadThroughput / MIN_IO_PER_SEGMENT);
        logger.debug("Would use {} segments for throughput", numSegmentsForThroughput);

        // Take the larger
        int numSegments = Math.max(numSegmentsForSize, numSegmentsForThroughput);

        // Fit to bounds
        numSegments = Math.min(numSegments, MAX_SCAN_SEGMENTS);
        numSegments = Math.max(numSegments, MIN_SCAN_SEGMENTS);

        logger.debug("Using computed number of segments: {}", numSegments);
        return numSegments;
    }

    /*
    Simple convenient holder for key data
     */
    private static class KeyNames
    {
        private String hashKey;
        private String rangeKey;

        private KeyNames(String hashKey, String rangeKey)
        {
            this.hashKey = hashKey;
            this.rangeKey = rangeKey;
        }

        private String getHashKey()
        {
            return hashKey;
        }

        private Optional<String> getRangeKey()
        {
            return Optional.ofNullable(rangeKey);
        }
    }
}
