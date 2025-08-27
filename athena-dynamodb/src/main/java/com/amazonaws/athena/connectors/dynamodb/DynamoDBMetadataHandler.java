/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2023 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants;
import com.amazonaws.athena.connectors.dynamodb.credentials.CrossAccountCredentialsProviderV2;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBPaginatedTables;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.qpt.DDBQueryPassthrough;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBTableResolver;
import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.athena.connectors.dynamodb.util.IncrementingValueNameProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.dynamodb.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
import static com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils.buildFilterPredicatesFromPlan;
import static com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils.SCHEMA_INFERENCE_NUM_RECORDS;

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
    private static final TableFilter TABLE_FILTER = (Table table) -> table.storageDescriptor().location().contains(DYNAMODB)
            || (table.parameters() != null && DYNAMODB.equals(table.parameters().get("classification")))
            || (table.storageDescriptor().parameters() != null && DYNAMODB.equals(table.storageDescriptor().parameters().get("classification")));
    // used to filter out Glue databases which lack the DYNAMO_DB_FLAG in the URI.
    private static final DatabaseFilter DB_FILTER = (Database database) -> (database.locationUri() != null && database.locationUri().contains(DYNAMO_DB_FLAG));

    private final ThrottlingInvoker invoker;
    private final DynamoDbClient ddbClient;
    private final GlueClient glueClient;
    private final DynamoDBTableResolver tableResolver;

    private final DDBQueryPassthrough queryPassthrough;

    public DynamoDBMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        this.ddbClient = DynamoDbClient.builder() 
                .credentialsProvider(CrossAccountCredentialsProviderV2.getCrossAccountCredentialsIfPresent(configOptions, "DynamoDBMetadataHandler_CrossAccountRoleSession"))
                .build();
        this.glueClient = getAwsGlue();
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
        this.queryPassthrough = new DDBQueryPassthrough();
    }

    @VisibleForTesting
    DynamoDBMetadataHandler(
            EncryptionKeyFactory keyFactory,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            String spillBucket,
            String spillPrefix,
            DynamoDbClient ddbClient,
            GlueClient glueClient,
            java.util.Map<String, String> configOptions)
    {
        super(glueClient, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glueClient = glueClient;
        this.ddbClient = ddbClient;
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.tableResolver = new DynamoDBTableResolver(invoker, ddbClient);
        this.queryPassthrough = new DDBQueryPassthrough();
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        this.queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, this.configOptions);
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));

        List<StandardFunctions> supportedFunctions = new ArrayList<>();
        supportedFunctions.add(StandardFunctions.AND_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.IS_NULL_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME);
        supportedFunctions.add(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME);
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(supportedFunctions.stream()
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
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
     * Pagination only implemented for DynamoDBTableResolver.listTables()
     * @see GlueMetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws Exception
    {
        // LinkedHashSet for consistent ordering
        Set<TableName> combinedTables = new LinkedHashSet<>();
        String token = request.getNextToken();
        if (token == null && glueClient != null) { // first invocation will get ALL glue tables in one shot
            try {
                // does not validate that the tables are actually DDB tables
                combinedTables.addAll(super.doListTables(allocator, new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(),
                                request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE), TABLE_FILTER).getTables());
            }
            catch (RuntimeException e) {
                logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }

        // future invocations will paginate on default ddb schema
        // add tables that may not be in Glue (if listing the default schema)
        if (DynamoDBConstants.DEFAULT_SCHEMA.equals(request.getSchemaName())) {
            FederatedIdentity federatedIdentity = request.getIdentity();
            AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(federatedIdentity.getConfigOptions());
            DynamoDBPaginatedTables ddbPaginatedResponse = tableResolver.listTables(request.getNextToken(),
                    request.getPageSize(), overrideConfig);
            List<TableName> tableNames = ddbPaginatedResponse.getTables().stream()
                    .map(table -> table.toLowerCase(Locale.ENGLISH)) // lowercase for compatibility
                    .map(table -> new TableName(DEFAULT_SCHEMA, table))
                    .collect(Collectors.toList());
            token = ddbPaginatedResponse.getToken();
            combinedTables.addAll(tableNames);
        }
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), token);
    }

    @Override
    public GetTableResponse doGetQueryPassthroughSchema(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        if (!request.isQueryPassthrough()) {
            throw new AthenaConnectorException("No Query passed through [{}]" + request, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).errorMessage("No Query passed through [{}]" + request).build());
        }

        queryPassthrough.verify(request.getQueryPassthroughArguments());
        String partiQLStatement = request.getQueryPassthroughArguments().get(DDBQueryPassthrough.QUERY);
        FederatedIdentity federatedIdentity = request.getIdentity();
        AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(federatedIdentity.getConfigOptions());
        ExecuteStatementRequest executeStatementRequest =
                ExecuteStatementRequest.builder()
                        .statement(partiQLStatement)
                        .limit(SCHEMA_INFERENCE_NUM_RECORDS)
                        .overrideConfiguration(overrideConfig)
                        .build();
        //PartiQL on DynamoDB Doesn't allow a dry run; therefore, we look "Peek" over the first few records
        ExecuteStatementResponse response = ddbClient.executeStatement(executeStatementRequest);
        SchemaBuilder schemaBuilder = DDBTableUtils.buildSchemaFromItems(response.items());

        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schemaBuilder.build(), Collections.emptySet());
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
        FederatedIdentity federatedIdentity = request.getIdentity();
        AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(federatedIdentity.getConfigOptions());
        // ignore database/schema name since there are no databases/schemas in DDB
        Schema schema = tableResolver.getTableSchema(request.getTableName().getTableName(), overrideConfig);
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
        if (request.getTableName().getQualifiedTableName().equalsIgnoreCase(queryPassthrough.getFunctionSignature())) {
            //Query passthrough does not support partition
            return;
        }
        FederatedIdentity federatedIdentity = request.getIdentity();
        AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(federatedIdentity.getConfigOptions());
        // use the source table name from the schema if available (in case Glue table name != actual table name)
        String tableName = getSourceTableName(request.getSchema());
        if (tableName == null) {
            tableName = request.getTableName().getTableName();
        }
        DynamoDBTable table = null;
        try {
            table = tableResolver.getTableMetadata(tableName, overrideConfig);
        }
        catch (TimeoutException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString()).errorMessage(e.getMessage()).build());
        }
        // add table name so we don't have to do case insensitive resolution again
        partitionSchemaBuilder.addMetadata(TABLE_METADATA, table.getName());
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        QueryPlan queryPlan = request.getConstraints().getQueryPlan();

        List<String> requestedCols = request.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        DynamoDBIndex index;
        Plan plan = null;
        Map<String, List<ColumnPredicate>> filterPredicates = new HashMap<>();
        boolean useQueryPlan = false;
        if (Objects.nonNull(queryPlan)) {
            plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
            filterPredicates = buildFilterPredicatesFromPlan(plan);
            index = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, requestedCols, filterPredicates);
            useQueryPlan = true;
        }
        else {
            index = DDBPredicateUtils.getBestIndexForPredicates(table, requestedCols, summary);
        }
        logger.info("using index: {}", index.getName());
        String hashKeyName = index.getHashKey();
        
        HashKeyPredicateInfo hashKeyInfo = extractHashKeyInfo(hashKeyName, summary, filterPredicates, useQueryPlan);

        DDBRecordMetadata recordMetadata = new DDBRecordMetadata(request.getSchema());

        Set<String> columnsToIgnore = new HashSet<>();
        List<AttributeValue> valueAccumulator = new ArrayList<>();
        IncrementingValueNameProducer valueNameProducer = new IncrementingValueNameProducer();
        
        if (!hashKeyInfo.isEmpty()) {
            // can "partition" on hash key
            setupQueryPartition(partitionSchemaBuilder, hashKeyName, hashKeyInfo.arrowType(), table, index, columnsToIgnore);

            setupRangeKeyFilter(partitionSchemaBuilder, index, summary, filterPredicates, useQueryPlan, 
                              valueAccumulator, valueNameProducer, recordMetadata, columnsToIgnore);
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

        precomputeAdditionalMetadata(columnsToIgnore, summary, valueAccumulator, valueNameProducer,
                partitionSchemaBuilder, recordMetadata, filterPredicates, useQueryPlan);
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
        FederatedIdentity federatedIdentity = request.getIdentity();
        AwsRequestOverrideConfiguration overrideConfig = getRequestOverrideConfig(federatedIdentity.getConfigOptions());
        // TODO consider caching this repeated work in #enhancePartitionSchema
        // use the source table name from the schema if available (in case Glue table name != actual table name)
        String tableName = getSourceTableName(request.getSchema());
        if (tableName == null) {
            tableName = request.getTableName().getTableName();
        }
        DynamoDBTable table = tableResolver.getTableMetadata(tableName, overrideConfig);

        List<String> requestedCols = request.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toList());
        QueryPlan queryPlan = request.getConstraints().getQueryPlan();
        Plan plan = null;
        boolean useQueryPlan = false;
        DynamoDBIndex index;
        Map<String, List<ColumnPredicate>>  filterPredicates = new HashMap<>();
        if (Objects.nonNull(queryPlan)) {
            plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
            filterPredicates = buildFilterPredicatesFromPlan(plan);
            index = DDBPredicateUtils.getBestIndexForPredicatesForPlan(table, requestedCols, filterPredicates);
            useQueryPlan = true;
        }
        else {
            Map<String, ValueSet> summary = request.getConstraints().getSummary();
            index = DDBPredicateUtils.getBestIndexForPredicates(table, requestedCols, summary);
        }

        String hashKeyName = index.getHashKey();
        List<Object> hashKeyValues = new ArrayList<>();
        if (useQueryPlan) {
            if (filterPredicates.get(hashKeyName) != null) {
                List<ColumnPredicate> columnPredicates = DDBPredicateUtils.getHashKeyAttributeValues(filterPredicates.get(hashKeyName));
                hashKeyValues = columnPredicates.stream()
                        .map(ColumnPredicate::getValue)
                        .collect(Collectors.toList());
            }
        }
        else {
            Map<String, ValueSet> summary = request.getConstraints().getSummary();
            ValueSet hashKeyValueSet = summary.get(hashKeyName);
            hashKeyValues = (hashKeyValueSet != null) ? DDBPredicateUtils.getHashKeyAttributeValues(hashKeyValueSet)
                    : Collections.emptyList();
        }

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
                                              IncrementingValueNameProducer valueNameProducer, SchemaBuilder partitionsSchemaBuilder, DDBRecordMetadata recordMetadata,
                                              Map<String, List<ColumnPredicate>> filterPredicates, boolean useQueryPlan)
    {
        // precompute non-key filter
        String filterExpression = null;
        if (!useQueryPlan) {
            filterExpression = DDBPredicateUtils.generateFilterExpression(columnsToIgnore, predicates, accumulator, valueNameProducer, recordMetadata);
        }
        else {
            filterExpression = DDBPredicateUtils.generateFilterExpressionForPlan(columnsToIgnore, filterPredicates, accumulator, valueNameProducer, recordMetadata);
        }

        if (filterExpression != null) {
            partitionsSchemaBuilder.addMetadata(NON_KEY_FILTER_METADATA, filterExpression);
        }

        if (!accumulator.isEmpty()) {
            // add in mappings for aliased columns and value placeholders
            Map<String, String> aliasedColumns = new HashMap<>();
            Set<String> filterColumns = new HashSet<>();
            if (useQueryPlan) {
                filterColumns = filterPredicates.keySet();
            }
            else {
                filterColumns = predicates.keySet();
            }
            for (String column : filterColumns) {
                aliasedColumns.put(DDBPredicateUtils.aliasColumn(column), column);
            }
            Map<String, AttributeValue> expressionValueMapping = new HashMap<>();
            // IncrementingValueNameProducer is repeatable for simplicity
            IncrementingValueNameProducer valueNameProducer2 = new IncrementingValueNameProducer();
            for (AttributeValue value : accumulator) {
                expressionValueMapping.put(valueNameProducer2.getNext(), value);
            }

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                partitionsSchemaBuilder.addMetadata(EXPRESSION_NAMES_METADATA, objectMapper.writeValueAsString(aliasedColumns));
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            
            partitionsSchemaBuilder.addMetadata(EXPRESSION_VALUES_METADATA, EnhancedDocument.fromAttributeValueMap(expressionValueMapping).toJson());
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
        if (request.getConstraints().isQueryPassThrough()) {
            logger.info("QPT Split Requested");
            return setupQueryPassthroughSplit(request);
        }

        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        Map<String, String> partitionMetadata = partitions.getSchema().getCustomMetadata();
        String partitionType = partitionMetadata.get(PARTITION_TYPE_METADATA);
        if (partitionType == null) {
            throw new AthenaConnectorException(String.format("No metadata %s defined in Schema %s", PARTITION_TYPE_METADATA, partitions.getSchema()), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
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
                splitMetadata.put(hashKeyName, DDBTypeUtils.attributeToJson(DDBTypeUtils.toAttributeValue(hashKeyValue), hashKeyName));

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
            throw new AthenaConnectorException("Unexpected partition type " + partitionType, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
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
    
    /**
     * Extracts hash key values and type information from constraints.
     */
    private HashKeyPredicateInfo extractHashKeyInfo(String hashKeyName, Map<String, ValueSet> summary,
                                                    Map<String, List<ColumnPredicate>> filterPredicates, boolean useQueryPlan)
    {
        if (useQueryPlan) {
            List<ColumnPredicate> predicates = filterPredicates.get(hashKeyName);
            if (predicates != null) {
                List<ColumnPredicate> hashKeyPredicates = DDBPredicateUtils.getHashKeyAttributeValues(predicates);
                if (!hashKeyPredicates.isEmpty()) {
                    return new HashKeyPredicateInfo(
                        Collections.singletonList(hashKeyPredicates),
                        hashKeyPredicates.get(0).getArrowType()
                    );
                }
            }
            return new HashKeyPredicateInfo(Collections.emptyList(), null);
        }
        else {
            ValueSet hashKeyValueSet = summary.get(hashKeyName);
            ArrowType arrowType = (hashKeyValueSet != null) ? hashKeyValueSet.getType() : null;
            List<Object> hashKeyValues = (hashKeyValueSet != null) ? 
                DDBPredicateUtils.getHashKeyAttributeValues(hashKeyValueSet) : Collections.emptyList();
            return new HashKeyPredicateInfo(hashKeyValues, arrowType);
        }
    }
    
    /**
     * Sets up query partition metadata in the schema builder.
     */
    private void setupQueryPartition(SchemaBuilder partitionSchemaBuilder, String hashKeyName, ArrowType arrowType,
                                   DynamoDBTable table, DynamoDBIndex index, Set<String> columnsToIgnore)
    {
        partitionSchemaBuilder.addField(hashKeyName, arrowType);
        partitionSchemaBuilder.addMetadata(HASH_KEY_NAME_METADATA, hashKeyName);
        columnsToIgnore.add(hashKeyName);
        partitionSchemaBuilder.addMetadata(PARTITION_TYPE_METADATA, QUERY_PARTITION_TYPE);
        
        if (!table.getName().equals(index.getName())) {
            partitionSchemaBuilder.addMetadata(INDEX_METADATA, index.getName());
        }
    }
    
    /**
     * Sets up range key filter if applicable.
     */
    private void setupRangeKeyFilter(SchemaBuilder partitionSchemaBuilder, DynamoDBIndex index,
                                   Map<String, ValueSet> summary, Map<String, List<ColumnPredicate>> filterPredicates,
                                   boolean useQueryPlan, List<AttributeValue> valueAccumulator,
                                   IncrementingValueNameProducer valueNameProducer, DDBRecordMetadata recordMetadata,
                                   Set<String> columnsToIgnore)
    {
        Optional<String> rangeKey = index.getRangeKey();
        if (rangeKey.isEmpty()) {
            return;
        }
        
        String rangeKeyName = rangeKey.get();
        String rangeKeyFilter = null;
        
        if (!useQueryPlan && summary.containsKey(rangeKeyName)) {
            rangeKeyFilter = DDBPredicateUtils.generateSingleColumnFilter(
                rangeKeyName, summary.get(rangeKeyName), valueAccumulator, valueNameProducer, recordMetadata, true);
        }
        else if (useQueryPlan && filterPredicates.containsKey(rangeKeyName)) {
            rangeKeyFilter = DDBPredicateUtils.generateSingleColumnFilter(
                rangeKeyName, filterPredicates.get(rangeKeyName), valueAccumulator, valueNameProducer, recordMetadata, true);
        }
        
        if (rangeKeyFilter != null) {
            logger.info("filter on range key is: {}", rangeKeyFilter);
            partitionSchemaBuilder.addMetadata(RANGE_KEY_NAME_METADATA, rangeKeyName);
            partitionSchemaBuilder.addMetadata(RANGE_KEY_FILTER_METADATA, rangeKeyFilter);
            columnsToIgnore.add(rangeKeyName);
        }
    }

    /**
     * Helper function that provides a single partition for Query Pass-Through
     *
     */
    private GetSplitsResponse setupQueryPassthroughSplit(GetSplitsRequest request)
    {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        //Since this is QPT query we return a fixed split.
        Map<String, String> qptArguments = request.getConstraints().getQueryPassthroughArguments();
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey())
                        .applyProperties(qptArguments)
                        .build());
    }

    /**
     * Helper class to encapsulate hash key information.
     */
    private static final class HashKeyPredicateInfo
    {
        private final List<Object> values;
        private final ArrowType arrowType;

        HashKeyPredicateInfo(List<Object> values, ArrowType arrowType)
        {
            this.values = values;
            this.arrowType = arrowType;
        }

        List<Object> values()
        {
            return values;
        }

        ArrowType arrowType()
        {
            return arrowType;
        }

        boolean isEmpty()
        {
            return values.isEmpty();
        }
    }
}
