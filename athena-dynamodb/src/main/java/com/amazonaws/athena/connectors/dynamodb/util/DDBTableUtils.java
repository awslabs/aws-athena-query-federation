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

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public static DynamoDBTable getTable(String tableName, AmazonDynamoDB ddbClient)
    {
        DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
        TableDescription table = ddbClient.describeTable(request).getTable();

        KeyNames keys = getKeys(table.getKeySchema());

        // get data statistics
        long approxTableSizeInBytes = table.getTableSizeBytes();
        long approxItemCount = table.getItemCount();
        final long provisionedReadCapacity = table.getProvisionedThroughput() != null ? table.getProvisionedThroughput().getReadCapacityUnits() : PSUEDO_CAPACITY_FOR_ON_DEMAND;

        // get secondary indexes
        List<LocalSecondaryIndexDescription> localSecondaryIndexes = table.getLocalSecondaryIndexes() != null ? table.getLocalSecondaryIndexes() : ImmutableList.of();
        List<GlobalSecondaryIndexDescription> globalSecondaryIndexes = table.getGlobalSecondaryIndexes() != null ? table.getGlobalSecondaryIndexes() : ImmutableList.of();
        ImmutableList.Builder<DynamoDBTable> indices = ImmutableList.builder();
        localSecondaryIndexes.forEach(i -> {
            KeyNames indexKeys = getKeys(i.getKeySchema());
            indices.add(new DynamoDBTable(i.getIndexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), ImmutableList.of(), i.getIndexSizeBytes(), i.getItemCount(),
                    provisionedReadCapacity));
        });
        globalSecondaryIndexes.forEach(i -> {
            KeyNames indexKeys = getKeys(i.getKeySchema());
            indices.add(new DynamoDBTable(i.getIndexName(), indexKeys.getHashKey(), indexKeys.getRangeKey(), ImmutableList.of(), i.getIndexSizeBytes(), i.getItemCount(),
                    i.getProvisionedThroughput() != null ? i.getProvisionedThroughput().getReadCapacityUnits() : PSUEDO_CAPACITY_FOR_ON_DEMAND));
        });

        return new DynamoDBTable(tableName, keys.getHashKey(), keys.getRangeKey(), indices.build(), approxTableSizeInBytes, approxItemCount, provisionedReadCapacity);
    }

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

    public static DynamoDBTable getBestIndexForPredicates(DynamoDBTable table, Map<String, ValueSet> constraintSummary)
    {
        Set<String> columnNames = constraintSummary.keySet();

        ImmutableList.Builder<DynamoDBTable> hashKeyMatchesBuilder = ImmutableList.builder();
        // if the original table has a hash key matching a predicate, start with that
        if (columnNames.contains(table.getHashKey())) {
            hashKeyMatchesBuilder.add(table);
        }

        // get indices with hash keys that match a predicate
        table.getIndexes().stream()
                .filter(index -> columnNames.contains(index.getHashKey()) && !getHashKeyAttributeValues(constraintSummary.get(index.getHashKey())).isEmpty())
                .forEach(hashKeyMatchesBuilder::add);
        List<DynamoDBTable> hashKeyMatches = hashKeyMatchesBuilder.build();

        // if the original table has a range key matching a predicate, start with that
        ImmutableList.Builder<DynamoDBTable> rangeKeyMatchesBuilder = ImmutableList.builder();
        if (table.getRangeKey().isPresent() && columnNames.contains(table.getRangeKey().get())) {
            rangeKeyMatchesBuilder.add(table);
        }

        // get indices with range keys that match a predicate
        table.getIndexes().stream()
                .filter(index -> index.getRangeKey().isPresent() && columnNames.contains(index.getRangeKey().get()))
                .forEach(rangeKeyMatchesBuilder::add);
        List<DynamoDBTable> rangeKeyMatches = rangeKeyMatchesBuilder.build();

        // return first index where both hash and range key can be specified with predicates
        for (DynamoDBTable index : hashKeyMatches) {
            if (rangeKeyMatches.contains(index)) {
                return index;
            }
        }
        // else return the first index with a hash key predicate, or the original table if there are none
        return hashKeyMatches.isEmpty() ? table : hashKeyMatches.get(0);
    }

    public static List<Object> getHashKeyAttributeValues(ValueSet valueSet)
    {
        if (valueSet.isSingleValue()) {
            return ImmutableList.of(valueSet.getSingleValue());
        }
        else if (valueSet instanceof SortedRangeSet) {
            List<Range> ranges = valueSet.getRanges().getOrderedRanges();
            ImmutableList.Builder<Object> attributeValues = ImmutableList.builder();
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    attributeValues.add(range.getSingleValue());
                }
                else {
                    // DDB Query can't handle non-equality conditions for the hash key
                    return ImmutableList.of();
                }
            }
            return attributeValues.build();
        }
        else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
            if (equatableValueSet.isWhiteList()) {
                ImmutableList.Builder<Object> values = ImmutableList.builder();
                for (int pos = 0; pos < equatableValueSet.getValueBlock().getRowCount(); pos++) {
                    values.add(equatableValueSet.getValue(pos));
                }
                return values.build();
            }
        }

        return ImmutableList.of();
    }

    public static Schema peekTableForSchema(String tableName, AmazonDynamoDB ddbClient)
    {
        ScanRequest scanRequest = new ScanRequest().withTableName(tableName).withLimit(SCHEMA_INFERENCE_NUM_RECORDS);
        ScanResult scanResult = ddbClient.scan(scanRequest);
        List<Map<String, AttributeValue>> items = scanResult.getItems();
        Set<String> discoveredColumns = new HashSet<>();
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        for (Map<String, AttributeValue> item : items) {
            for (Map.Entry<String, AttributeValue> column : item.entrySet()) {
                if (!discoveredColumns.contains(column.getKey()) && !Boolean.TRUE.equals(column.getValue().getNULL())) {
                    schemaBuilder.addField(DDBTypeUtils.getArrowField(column.getKey(), ItemUtils.toSimpleValue(column.getValue())));
                    discoveredColumns.add(column.getKey());
                }
            }
        }
        return schemaBuilder.build();
    }

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

    // TODO handle case where different attributes in different records may have different types
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
