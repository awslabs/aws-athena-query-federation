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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static DynamoDBTable getTable(String tableName, ThrottlingInvoker invoker, AmazonDynamoDB ddbClient)
            throws TimeoutException
    {
        DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
        TableDescription table = invoker.invoke(() -> ddbClient.describeTable(request).getTable());

        KeyNames keys = getKeys(table.getKeySchema());

        // get data statistics
        long approxTableSizeInBytes = table.getTableSizeBytes();
        long approxItemCount = table.getItemCount();
        final long provisionedReadCapacity = table.getProvisionedThroughput() != null ? table.getProvisionedThroughput().getReadCapacityUnits() : PSUEDO_CAPACITY_FOR_ON_DEMAND;

        // get secondary indexes
        List<LocalSecondaryIndexDescription> localSecondaryIndexes = table.getLocalSecondaryIndexes() != null ? table.getLocalSecondaryIndexes() : ImmutableList.of();
        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = table.getGlobalSecondaryIndexes() != null ? table.getGlobalSecondaryIndexes() : ImmutableList.of();
        ImmutableList.Builder<DynamoDBIndex> indices = ImmutableList.builder();
        localSecondaryIndexes.forEach(i -> {
            KeyNames indexKeys = getKeys(i.getKeySchema());
            // DynamoDB automatically fetches all attributes from the table for local secondary index, so ignore projected attributes
            indices.add(new DynamoDBIndex(i.getIndexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), ProjectionType.ALL, ImmutableList.of()));
        });
        globalSecondaryIndexes.stream()
              .filter(i -> IndexStatus.fromValue(i.getIndexStatus()).equals(IndexStatus.ACTIVE))
              .forEach(i -> {
                  KeyNames indexKeys = getKeys(i.getKeySchema());
                  indices.add(new DynamoDBIndex(i.getIndexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), ProjectionType.fromValue(i.getProjection().getProjectionType()),
                        i.getProjection().getNonKeyAttributes() == null ? ImmutableList.of() : i.getProjection().getNonKeyAttributes()));
              });

        return new DynamoDBTable(tableName, keys.getHashKey(), keys.getRangeKey(), table.getAttributeDefinitions(), indices.build(), approxTableSizeInBytes, approxItemCount, provisionedReadCapacity);
    }

    /*
    Parses the key attributes from the given list of KeySchemaElements
     */
    private static KeyNames getKeys(List<KeySchemaElement> keys)
    {
        String hashKey = null;
        String rangeKey = null;
        for (KeySchemaElement key : keys) {
            if (key.getKeyType().equals(KeyType.HASH.toString())) {
                hashKey = key.getAttributeName();
            }
            else if (key.getKeyType().equals(KeyType.RANGE.toString())) {
                rangeKey = key.getAttributeName();
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
    public static Schema peekTableForSchema(String tableName, ThrottlingInvoker invoker, AmazonDynamoDB ddbClient)
            throws TimeoutException
    {
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withLimit(SCHEMA_INFERENCE_NUM_RECORDS);
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        try {
            ScanResult scanResult = invoker.invoke(() -> ddbClient.scan(scanRequest));
            List<Map<String, AttributeValue>> items = scanResult.getItems();
            Set<String> discoveredColumns = new HashSet<>();
            if (!items.isEmpty()) {
                for (Map<String, AttributeValue> item : items) {
                    for (Map.Entry<String, AttributeValue> column : item.entrySet()) {
                        if (!discoveredColumns.contains(column.getKey())) {
                            Field field = DDBTypeUtils.inferArrowField(column.getKey(), ItemUtils.toSimpleValue(column.getValue()));
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
                    schemaBuilder.addField(DDBTypeUtils.getArrowFieldFromDDBType(attributeDefinition.getAttributeName(), attributeDefinition.getAttributeType()));
                }
            }
        }
        catch (AmazonDynamoDBException amazonDynamoDBException) {
            if (amazonDynamoDBException.getMessage().contains("AWSKMSException")) {
                logger.warn("Failed to retrieve table schema due to KMS issue, empty schema for table: {}. Error Message: {}", tableName, amazonDynamoDBException.getMessage());
            }
            else {
                throw amazonDynamoDBException;
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
