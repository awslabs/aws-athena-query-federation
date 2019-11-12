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

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBTableResolver;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.athena.connectors.dynamodb.util.IncrementingValueNameProducer;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.PARTITION_TYPE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.QUERY_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SCAN_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.throttling.DynamoDBExceptionFilter.EXCEPTION_FILTER;

public class DynamoDBMetadataHandler
        extends GlueMetadataHandler
{
    @VisibleForTesting
    static final int MAX_SPLITS_PER_REQUEST = 1000;
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandler.class);
    private static final String sourceType = "ddb";
    private static final String GLUE_ENV = "disable_glue";

    private final ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
    private final AmazonDynamoDB ddbClient;
    private final AWSGlue glueClient;
    private final DynamoDBTableResolver tableResolver;

    public DynamoDBMetadataHandler()
    {
        super((System.getenv(GLUE_ENV) == null || !Boolean.parseBoolean(System.getenv(GLUE_ENV))) ? AWSGlueClientBuilder.standard().build() : null,
                sourceType);
        ddbClient = AmazonDynamoDBClientBuilder.standard().build();
        glueClient = getAwsGlue();
        tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
    }

    @VisibleForTesting
    DynamoDBMetadataHandler(EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix,
            AmazonDynamoDB ddbClient,
            AWSGlue glueClient)
    {
        super(glueClient, keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        this.glueClient = glueClient;
        this.ddbClient = ddbClient;
        this.tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        if (glueClient != null) {
            try {
                return super.doListSchemaNames(allocator, request);
            }
            catch (RuntimeException e) {
                logger.debug("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            }
        }

        return new ListSchemasResponse(request.getCatalogName(), ImmutableList.of("default"));
    }

    /*
    TODO add table whitelist
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws Exception
    {
        // LinkedHashSet for consistent ordering
        Set<TableName> combinedTables = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                // does not validate that the tables are actually DDB tables
                combinedTables.addAll(super.doListTables(allocator, request).getTables());
            }
            catch (RuntimeException e) {
                logger.debug("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }

        // add tables that may not be in Glue (if listing the default schema)
        if (DynamoDBConstants.DEFAULT_SCHEMA.equals(request.getSchemaName())) {
            combinedTables.addAll(tableResolver.listTables());
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables));
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
            throws Exception
    {
        if (glueClient != null) {
            try {
                // does not validate that the table is actually a DDB table
                return super.doGetTable(allocator, request);
            }
            catch (RuntimeException e) {
                logger.debug("doGetTable: Unable to retrieve table {} from AWSGlue in database/schema {}", request.getTableName().getSchemaName(), e);
            }
        }

        // ignore database/schema name since there are no databases/schemas in DDB
        Schema schema = tableResolver.getTableSchema(request.getTableName().getTableName());
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        DynamoDBTable table = null;
        try {
            table = tableResolver.getTableMetadata(request.getTableName().getTableName());
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        // add table name so we don't have to do case insensitive resolution again
        partitionSchemaBuilder.addMetadata(TABLE_METADATA, table.getName());
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        DynamoDBTable index = DDBPredicateUtils.getBestIndexForPredicates(table, summary);
        String hashKeyName = index.getHashKey();
        ValueSet hashKeyValueSet = summary.get(hashKeyName);
        List<Object> hashKeyValues = (hashKeyValueSet != null) ? DDBPredicateUtils.getHashKeyAttributeValues(hashKeyValueSet) : Collections.emptyList();

        Set<String> alreadyFilteredColumns = new HashSet<>();
        List<AttributeValue> valueAccumulator = new ArrayList<>();
        IncrementingValueNameProducer valueNameProducer = new IncrementingValueNameProducer();
        if (!hashKeyValues.isEmpty()) {
            // can "partition" on hash key
            partitionSchemaBuilder.addField(hashKeyName, hashKeyValueSet.getType());
            partitionSchemaBuilder.addMetadata(HASH_KEY_NAME_METADATA, hashKeyName);
            alreadyFilteredColumns.add(hashKeyName);
            partitionSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, QUERY_PARTITION_TYPE);
            if (!table.equals(index)) {
                partitionSchemaBuilder.addMetadata(INDEX_METADATA, index.getName());
            }

            // add range key filter if there is one
            Optional<String> rangeKey = index.getRangeKey();
            if (rangeKey.isPresent()) {
                String rangeKeyName = rangeKey.get();
                if (summary.containsKey(rangeKeyName)) {
                    String rangeKeyFilter = DDBPredicateUtils.generateSingleColumnFilter(rangeKeyName, summary.get(rangeKeyName), valueAccumulator, valueNameProducer);
                    partitionSchemaBuilder.addMetadata(RANGE_KEY_NAME_METADATA, rangeKeyName);
                    partitionSchemaBuilder.addMetadata(RANGE_KEY_FILTER_METADATA, rangeKeyFilter);
                    alreadyFilteredColumns.add(rangeKeyName);
                }
            }
        }
        else {
            // always fall back to a scan
            partitionSchemaBuilder.addField(SEGMENT_COUNT_METADATA, Types.MinorType.INT.getType());
            partitionSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, SCAN_PARTITION_TYPE);
        }

        precomputeAdditionalMetadata(alreadyFilteredColumns, summary, valueAccumulator, valueNameProducer, partitionSchemaBuilder);
    }

    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request)
            throws Exception
    {
        // TODO consider caching this repeated work in #enhancePartitionSchema
        DynamoDBTable table = tableResolver.getTableMetadata(request.getTableName().getTableName());
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        DynamoDBTable index = DDBPredicateUtils.getBestIndexForPredicates(table, summary);
        String hashKeyName = index.getHashKey();
        ValueSet hashKeyValueSet = summary.get(hashKeyName);
        List<Object> hashKeyValues = (hashKeyValueSet != null) ? DDBPredicateUtils.getHashKeyAttributeValues(hashKeyValueSet) : Collections.emptyList();

        if (!hashKeyValues.isEmpty()) {
            for (Object hashKeyValue : hashKeyValues) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(hashKeyName, rowNum, hashKeyValue);
                    //we added 1 partition per hashkey value
                    return 1;
                });
            }
        }
        else {
            // always fall back to a scan, need to return at least one partition so stick the segment count in it
            int segmentCount = DDBTableUtils.getNumSegments(table.getProvisionedReadCapacity(), table.getApproxTableSizeInBytes());
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(SEGMENT_COUNT_METADATA, rowNum, segmentCount);
                return 1;
            });
        }
    }

    private void precomputeAdditionalMetadata(Set<String> columnsToIgnore, Map<String, ValueSet> predicates, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer, SchemaBuilder partitionsSchemaBuilder)
    {
        // precompute non-key filter
        String filterExpression = DDBPredicateUtils.generateFilterExpression(columnsToIgnore, predicates, accumulator, valueNameProducer);
        if (filterExpression != null) {
            partitionsSchemaBuilder.addMetadata(NON_KEY_FILTER_METADATA, filterExpression);
        }

        if (!accumulator.isEmpty()) {
            // add in mappings for aliased columns and value placeholders
            Map<String, String> aliasedColumns = new HashMap<>();
            for (String column : predicates.keySet()) {
                aliasedColumns.put(DDBPredicateUtils.aliasColumn(column), column);
            }
            Map<String, AttributeValue> expressionValueMapping = new HashMap<>();
            // IncrementingValueNameProducer is repeatable for simplicity
            IncrementingValueNameProducer valueNameProducer2 = new IncrementingValueNameProducer();
            for (AttributeValue value : accumulator) {
                expressionValueMapping.put(valueNameProducer2.getNext(), value);
            }
            partitionsSchemaBuilder.addMetadata(EXPRESSION_NAMES_METADATA, Jackson.toJsonString(aliasedColumns));
            partitionsSchemaBuilder.addMetadata(EXPRESSION_VALUES_METADATA, Jackson.toJsonString(expressionValueMapping));
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        Map<String, String> partitionMetadata = partitions.getSchema().getCustomMetadata();
        // copy all partition metadata to the split
        Map<String, String> splitMetadata = new HashMap<>(partitionMetadata);
        String partitionType = partitionMetadata.get(PARTITION_TYPE_METADATA);
        if (partitionType == null) {
            throw new IllegalStateException(String.format("No metadata %s defined in Schema %s", PARTITION_TYPE_METADATA, partitions.getSchema()));
        }
        if (QUERY_PARTITION_TYPE.equals(partitionType)) {
            String hashKeyName = partitionMetadata.get(HASH_KEY_NAME_METADATA);
            FieldReader hashKeyValueReader = partitions.getFieldReader(hashKeyName);
            // one split per hash key value (since one DDB query can only take one hash key value)
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                hashKeyValueReader.setPosition(curPartition);

                //Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(request);

                Object hashKeyValue = DDBTypeUtils.convertArrowTypeIfNecessary(hashKeyValueReader.readObject());
                String hashKeyValueJSON = Jackson.toJsonString(ItemUtils.toAttributeValue(hashKeyValue));
                splitMetadata.put(hashKeyName, hashKeyValueJSON);

                splitMetadata.put(SEGMENT_COUNT_METADATA, String.valueOf(partitions.getRowCount()));

                splits.add(new Split(spillLocation, makeEncryptionKey(), splitMetadata));

                if (splits.size() == MAX_SPLITS_PER_REQUEST && curPartition != partitions.getRowCount() - 1) {
                    // We've reached max page size and this is not the last partition
                    // so send the page back
                    return new GetSplitsResponse(request.getCatalogName(),
                            splits,
                            encodeContinuationToken(curPartition));
                }
            }
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
        else if (SCAN_PARTITION_TYPE.equals(partitionType)) {
            FieldReader segmentCountReader = partitions.getFieldReader(SEGMENT_COUNT_METADATA);
            int segmentCount = segmentCountReader.readInteger();
            for (int curPartition = partitionContd; curPartition < segmentCount; curPartition++) {
                SpillLocation spillLocation = makeSpillLocation(request);

                splitMetadata.put(SEGMENT_ID_PROPERTY, String.valueOf(curPartition));
                splitMetadata.put(SEGMENT_COUNT_METADATA, String.valueOf(segmentCount));

                splits.add(new Split(spillLocation, makeEncryptionKey(), splitMetadata));

                if (splits.size() == MAX_SPLITS_PER_REQUEST && curPartition != segmentCount - 1) {
                    // We've reached max page size and this is not the last partition
                    // so send the page back
                    return new GetSplitsResponse(request.getCatalogName(),
                            splits,
                            encodeContinuationToken(curPartition));
                }
            }
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
        else {
            throw new IllegalStateException("Unexpected partition type " + partitionType);
        }
    }

    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken()) + 1;
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
