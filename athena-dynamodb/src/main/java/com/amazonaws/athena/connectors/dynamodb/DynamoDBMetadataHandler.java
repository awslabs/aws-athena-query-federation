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

import com.amazonaws.athena.connector.credentials.CrossAccountCredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
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
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBTableResolver;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.athena.connectors.dynamodb.util.IncrementingValueNameProducer;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.util.json.Jackson;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
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

/**
 * Handles metadata requests for the Athena DynamoDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Glue DataCatalog is used for schema information by default unless disabled. If disabled or the table<br>
 * is not found, it falls back to doing a small table scan and derives a schema from that.<br>
 * 2. Determines if the data splits will need to perform DDB Queries or Scans.<br>
 * 3. Splits up the hash key into distinct Query splits if possible, otherwise falls back to creating Scan splits.<br>
 * 4. Also determines the best index to use (if available) if the available predicates align with Key Attributes.<br>
 * 5. Creates scan splits that support Parallel Scan and tries to choose the optimal number of splits.<br>
 * 6. Pushes down all other predicates into ready-to-use filter expressions to pass to DDB.
 */
public class DynamoDBMetadataHandler
        extends GlueMetadataHandler
{
    @VisibleForTesting
    static final int MAX_SPLITS_PER_REQUEST = 1000;
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandler.class);
    static final String DYNAMODB = "dynamodb";
    private static final String SOURCE_TYPE = "ddb";
    // defines the value that should be present in the Glue Database URI to enable the DB for DynamoDB.
    static final String DYNAMO_DB_FLAG = "dynamo-db-flag";
    // used to filter out Glue tables which lack indications of being used for DDB.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getStorageDescriptor().getLocation().contains(DYNAMODB)
            || (table.getParameters() != null && DYNAMODB.equals(table.getParameters().get("classification")))
            || (table.getStorageDescriptor().getParameters() != null && DYNAMODB.equals(table.getStorageDescriptor().getParameters().get("classification")));
    // used to filter out Glue databases which lack the DYNAMO_DB_FLAG in the URI.
    private static final DatabaseFilter DB_FILTER = (Database database) -> (database.getLocationUri() != null && database.getLocationUri().contains(DYNAMO_DB_FLAG));

    private final ThrottlingInvoker invoker;
    private final AmazonDynamoDB ddbClient;
    private final AWSGlue glueClient;
    private final DynamoDBTableResolver tableResolver;

    public DynamoDBMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.ddbClient = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(CrossAccountCredentialsProvider.getCrossAccountCredentialsIfPresent(configOptions, "DynamoDBMetadataHandler_CrossAccountRoleSession"))
            .build();
        this.glueClient = getAwsGlue();
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
    }

    @VisibleForTesting
    DynamoDBMetadataHandler(
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        AmazonDynamoDB ddbClient,
        AWSGlue glueClient,
        java.util.Map<String, String> configOptions)
    {
        super(glueClient, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glueClient = glueClient;
        this.ddbClient = ddbClient;
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
    }

    /**
     * Since DynamoDB does not have "schemas" or "databases", this lists all the Glue databases (if not
     * disabled) that contain {@value #DYNAMO_DB_FLAG} in their URIs . Otherwise returns just a "default" schema.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        Set<String> combinedSchemas = new LinkedHashSet<>();
        if (glueClient != null) {
            try {
                combinedSchemas.addAll(super.doListSchemaNames(allocator, request, DB_FILTER).getSchemas());
            }
            catch (RuntimeException e) {
                logger.warn("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            }
        }

        combinedSchemas.add(DEFAULT_SCHEMA);
        return new ListSchemasResponse(request.getCatalogName(), combinedSchemas);
    }

    /**
     * Lists all Glue tables (if not disabled) in the schema specified that indicate use for DynamoDB metadata.
     * Indications for DynamoDB use in Glue are:<br>
     * 1. The top level table properties/parameters contains a key called "classification" with value {@value #DYNAMODB}.<br>
     * 2. Or the storage descriptor's location field contains {@value #DYNAMODB}.<br>
     * 3. Or the storage descriptor has a parameter called "classification" with value {@value #DYNAMODB}.
     * <p>
     * If the specified schema is "default", this also returns an intersection with actual tables in DynamoDB.
     *
     * @see GlueMetadataHandler
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
                combinedTables.addAll(super.doListTables(allocator,
                        new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(),
                                request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE),
                        TABLE_FILTER).getTables());
            }
            catch (RuntimeException e) {
                logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }

        // add tables that may not be in Glue (if listing the default schema)
        if (DynamoDBConstants.DEFAULT_SCHEMA.equals(request.getSchemaName())) {
            combinedTables.addAll(tableResolver.listTables());
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), null);
    }

    /**
     * Fetches a table's schema from Glue DataCatalog if present and not disabled, otherwise falls
     * back to doing a small table scan derives a schema from that.
     *
     * @see GlueMetadataHandler
     */
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
                logger.warn("doGetTable: Unable to retrieve table {} from AWSGlue in database/schema {}. " +
                                "Falling back to schema inference. If inferred schema is incorrect, create " +
                                "a matching table in Glue to define schema (see README)",
                        request.getTableName().getTableName(), request.getTableName().getSchemaName(), e);
            }
        }

        // ignore database/schema name since there are no databases/schemas in DDB
        Schema schema = tableResolver.getTableSchema(request.getTableName().getTableName());
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    /**
     * Generates a partition schema with metadata derived from available predicates.  This metadata will be
     * copied to splits in the #doGetSplits call.  At this point it is determined whether we can partition
     * by hash key or fall back to a full table scan.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        // use the source table name from the schema if available (in case Glue table name != actual table name)
        String tableName = getSourceTableName(request.getSchema());
        if (tableName == null) {
            tableName = request.getTableName().getTableName();
        }
        DynamoDBTable table = null;
        try {
            table = tableResolver.getTableMetadata(tableName);
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        // add table name so we don't have to do case insensitive resolution again
        partitionSchemaBuilder.addMetadata(TABLE_METADATA, table.getName());
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        List<String> requestedCols = request.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        DynamoDBIndex index = DDBPredicateUtils.getBestIndexForPredicates(table, requestedCols, summary);
        logger.info("using index: {}", index.getName());
        String hashKeyName = index.getHashKey();
        ValueSet hashKeyValueSet = summary.get(hashKeyName);
        List<Object> hashKeyValues = (hashKeyValueSet != null) ? DDBPredicateUtils.getHashKeyAttributeValues(hashKeyValueSet) : Collections.emptyList();

        DDBRecordMetadata recordMetadata = new DDBRecordMetadata(request.getSchema());

        Set<String> columnsToIgnore = new HashSet<>();
        List<AttributeValue> valueAccumulator = new ArrayList<>();
        IncrementingValueNameProducer valueNameProducer = new IncrementingValueNameProducer();
        if (!hashKeyValues.isEmpty()) {
            // can "partition" on hash key
            partitionSchemaBuilder.addField(hashKeyName, hashKeyValueSet.getType());
            partitionSchemaBuilder.addMetadata(HASH_KEY_NAME_METADATA, hashKeyName);
            columnsToIgnore.add(hashKeyName);
            partitionSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, QUERY_PARTITION_TYPE);
            if (!table.getName().equals(index.getName())) {
                partitionSchemaBuilder.addMetadata(INDEX_METADATA, index.getName());
            }

            // add range key filter if there is one
            Optional<String> rangeKey = index.getRangeKey();
            if (rangeKey.isPresent()) {
                String rangeKeyName = rangeKey.get();
                if (summary.containsKey(rangeKeyName)) {
                    String rangeKeyFilter = DDBPredicateUtils.generateSingleColumnFilter(rangeKeyName, summary.get(rangeKeyName), valueAccumulator, valueNameProducer, recordMetadata, true);
                    partitionSchemaBuilder.addMetadata(RANGE_KEY_NAME_METADATA, rangeKeyName);
                    partitionSchemaBuilder.addMetadata(RANGE_KEY_FILTER_METADATA, rangeKeyFilter);
                    columnsToIgnore.add(rangeKeyName);
                }
            }
        }
        else {
            // always fall back to a scan
            partitionSchemaBuilder.addField(SEGMENT_COUNT_METADATA, Types.MinorType.INT.getType());
            partitionSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, SCAN_PARTITION_TYPE);
        }

        // We will exclude the columns with custom types from filter clause when querying/scanning DDB
        // As those types are not natively supported by DDB or Glue
        // So we have to filter the results after the query/scan result is returned
        columnsToIgnore.addAll(recordMetadata.getNonComparableColumns());

        precomputeAdditionalMetadata(columnsToIgnore, summary, valueAccumulator, valueNameProducer, partitionSchemaBuilder, recordMetadata);
    }

    /**
     * Generates hash key partitions if possible or generates a single partition with the heuristically
     * determined optimal scan segment count specified inside of it
     *
     * @see GlueMetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        // TODO consider caching this repeated work in #enhancePartitionSchema
        // use the source table name from the schema if available (in case Glue table name != actual table name)
        String tableName = getSourceTableName(request.getSchema());
        if (tableName == null) {
            tableName = request.getTableName().getTableName();
        }
        DynamoDBTable table = tableResolver.getTableMetadata(tableName);
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        List<String> requestedCols = request.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        DynamoDBIndex index = DDBPredicateUtils.getBestIndexForPredicates(table, requestedCols, summary);
        logger.info("using index: {}", index.getName());
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

    /*
    Injects additional metadata into the partition schema like a non-key filter expression for additional DDB-side filtering
     */
    private void precomputeAdditionalMetadata(Set<String> columnsToIgnore, Map<String, ValueSet> predicates, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer, SchemaBuilder partitionsSchemaBuilder, DDBRecordMetadata recordMetadata)
    {
        // precompute non-key filter
        String filterExpression = DDBPredicateUtils.generateFilterExpression(columnsToIgnore, predicates, accumulator, valueNameProducer, recordMetadata);
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

    /**
     * Copies data from partitions and creates splits, serializing as necessary for later calls to RecordHandler#readWithContraint.
     * This API supports pagination.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        Map<String, String> partitionMetadata = partitions.getSchema().getCustomMetadata();
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

                // copy all partition metadata to the split
                Map<String, String> splitMetadata = new HashMap<>(partitionMetadata);

                Object hashKeyValue = DDBTypeUtils.convertArrowTypeIfNecessary(hashKeyName, hashKeyValueReader.readObject());
                String hashKeyValueJSON = Jackson.toJsonString(ItemUtils.toAttributeValue(hashKeyValue));
                splitMetadata.put(hashKeyName, hashKeyValueJSON);

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
                //Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(request);

                // copy all partition metadata to the split
                Map<String, String> splitMetadata = new HashMap<>(partitionMetadata);

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

    /**
     * @see GlueMetadataHandler
     */
    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    /*
    Used to handle paginated requests. Returns the partition number to resume with.
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken()) + 1;
        }

        //No continuation token present
        return 0;
    }

    /*
    Used to create pagination tokens by encoding the number of the next partition to process.
     */
    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
