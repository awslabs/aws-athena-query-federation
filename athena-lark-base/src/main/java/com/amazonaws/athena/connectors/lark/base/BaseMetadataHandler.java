/*-
 * #%L
 * athena-example
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.lark.base.metadataProvider.ExperimentalMetadataProvider;
import com.amazonaws.athena.connectors.lark.base.metadataProvider.LarkSourceMetadataProvider;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.AthenaLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.PartitionInfoResult;
import com.amazonaws.athena.connectors.lark.base.model.TableDirectInitialized;
import com.amazonaws.athena.connectors.lark.base.model.TableSchemaResult;
import com.amazonaws.athena.connectors.lark.base.resolver.LarkBaseTableResolver;
import com.amazonaws.athena.connectors.lark.base.service.AthenaService;
import com.amazonaws.athena.connectors.lark.base.service.EnvVarService;
import com.amazonaws.athena.connectors.lark.base.service.GlueCatalogService;
import com.amazonaws.athena.connectors.lark.base.service.LarkBaseService;
import com.amazonaws.athena.connectors.lark.base.service.LarkDriveService;
import com.amazonaws.athena.connectors.lark.base.translator.SearchApiFilterTranslator;
import com.amazonaws.athena.connectors.lark.base.util.CommonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.BASE_ID_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.EXPECTED_ROW_COUNT_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.FILTER_EXPRESSION_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.IS_PARALLEL_SPLIT_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_BASE_FLAG;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.LARK_FIELD_TYPE_MAPPING_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.PAGE_SIZE;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.PAGE_SIZE_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_SPLIT_KEY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SORT_EXPRESSION_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SOURCE_TYPE;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SPLIT_END_INDEX_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.SPLIT_START_INDEX_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.BaseConstants.TABLE_ID_PROPERTY;
import static com.amazonaws.athena.connectors.lark.base.throttling.BaseExceptionFilter.EXCEPTION_FILTER;
import static java.util.Objects.requireNonNull;

/**
 * This class is responsible for providing metadata for the tables and schemas in the Lark Base data source.
 * This class extends the GlueMetadataHandler which provides a default implementation of the Glue Data Catalog
 * metadata handling. This class overrides the methods that are specific to the Lark Base data source.
 */
public class BaseMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BaseMetadataHandler.class);
    private static final DatabaseFilter DB_FILTER = (Database database) -> {
        if (database.locationUri() != null) {
            return database.locationUri().contains(LARK_BASE_FLAG);
        }
        return false;
    };
    private static final TableFilter TABLE_FILTER = (Table table) -> table.parameters().get("classification").contains(LARK_BASE_FLAG);

    private EnvVarService envVarService;
    private LarkBaseService larkBaseService;
    private GlueCatalogService glueCatalogService;
    private ThrottlingInvoker invoker;

    // Map to store discovered databases and their table
    // Map<databaseName, Map<tableName, Set<columnNames>>>
    private List<TableDirectInitialized> mappingTableDirectInitialized;
    private LarkSourceMetadataProvider larkSourceMetadataProvider;
    private ExperimentalMetadataProvider experimentalMetadataProvider;

    /**
     * Initialize Lark services by retrieving app credentials from AWS Secrets Manager.
     * This is common code used by both constructors.
     */
    private void initializeLarkServices(Map<String, String> configOptions)
    {
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
        this.envVarService = new EnvVarService(configOptions, invoker);
        AthenaService athenaService = new AthenaService();
        this.larkBaseService = new LarkBaseService(envVarService.getLarkAppId(), envVarService.getLarkAppSecret());
        LarkDriveService larkDriveService = new LarkDriveService(envVarService.getLarkAppId(), envVarService.getLarkAppSecret());
        this.glueCatalogService = new GlueCatalogService(getAwsGlue());
        LarkBaseTableResolver larkBaseTableResolver = new LarkBaseTableResolver(
                this.envVarService,
                this.larkBaseService,
                larkDriveService,
                this.invoker
        );
        this.mappingTableDirectInitialized = larkBaseTableResolver.resolveTables();
        this.experimentalMetadataProvider = new ExperimentalMetadataProvider(athenaService, larkBaseService, invoker);
        this.larkSourceMetadataProvider = new LarkSourceMetadataProvider(mappingTableDirectInitialized);
        if (envVarService.isEnableDebugLogging()) {
            logger.info("Initialization complete. Discovered {} target databases from metadata tables.", mappingTableDirectInitialized.size());
        }
    }

    public BaseMetadataHandler(Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        initializeLarkServices(configOptions);
    }

    /**
     * Constructs a new BaseMetadataHandler instance.
     *
     * @param glueClient The AWS Glue client.
     * @param keyFactory The encryption key factory.
     * @param awsSecretsManager The AWS Secrets Manager client.
     * @param athena The AWS Athena client.
     * @param spillBucket The S3 bucket for spilling results.
     * @param spillPrefix The S3 prefix for spilling results.
     * @param configOptions Configuration options for the connector.
     */
    @VisibleForTesting
    protected BaseMetadataHandler(
            GlueClient glueClient,
            EncryptionKeyFactory keyFactory,
            SecretsManagerClient awsSecretsManager,
            AthenaClient athena,
            String spillBucket,
            String spillPrefix,
            Map<String, String> configOptions)
    {
        super(glueClient, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        initializeLarkServices(configOptions);
    }

    /**
     * Full dependency injection constructor for testing purposes.
     * This constructor allows all dependencies to be injected, making the class fully testable without
     * requiring environment variables or real AWS services.
     *
     * @param glueClient The AWS Glue client.
     * @param keyFactory The encryption key factory.
     * @param awsSecretsManager The AWS Secrets Manager client.
     * @param athena The AWS Athena client.
     * @param spillBucket The S3 bucket for spilling results.
     * @param spillPrefix The S3 prefix for spilling results.
     * @param configOptions Configuration options for the connector.
     * @param envVarService Service for accessing environment variables.
     * @param larkBaseService Service for interacting with Lark Base API.
     * @param glueCatalogService Service for Glue catalog operations.
     * @param mappingTableDirectInitialized List of direct table mappings.
     * @param larkSourceMetadataProvider Provider for Lark source metadata.
     * @param experimentalMetadataProvider Provider for experimental metadata features.
     * @param invoker Throttling invoker for rate limiting.
     */
    @VisibleForTesting
    protected BaseMetadataHandler(
            GlueClient glueClient,
            EncryptionKeyFactory keyFactory,
            SecretsManagerClient awsSecretsManager,
            AthenaClient athena,
            String spillBucket,
            String spillPrefix,
            Map<String, String> configOptions,
            EnvVarService envVarService,
            LarkBaseService larkBaseService,
            GlueCatalogService glueCatalogService,
            List<TableDirectInitialized> mappingTableDirectInitialized,
            LarkSourceMetadataProvider larkSourceMetadataProvider,
            ExperimentalMetadataProvider experimentalMetadataProvider,
            ThrottlingInvoker invoker)
    {
        super(glueClient, keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.envVarService = envVarService;
        this.larkBaseService = larkBaseService;
        this.glueCatalogService = glueCatalogService;
        this.mappingTableDirectInitialized = mappingTableDirectInitialized;
        this.larkSourceMetadataProvider = larkSourceMetadataProvider;
        this.experimentalMetadataProvider = experimentalMetadataProvider;
        this.invoker = invoker;
    }

    /**
     * Used to get the list of schemas (aka databases) that this source contains, filtering by the DB_FILTER if defined.
     * It attempts to retrieve schemas primarily from AWS Glue.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return A ListSchemasResponse which primarily contains a Set<String> of schema names and a catalog name
     * corresponding the Athena catalog that was queried.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        if (envVarService.isEnableDebugLogging()) {
            logger.info("doListSchemaNames: enter - {}", request);
        }

        Set<String> schemas = new HashSet<>();

        try {
            ListSchemasResponse glueResponse = super.doListSchemaNames(allocator, request, DB_FILTER);
            schemas.addAll(glueResponse.getSchemas());
        }
        catch (Exception e) {
            logger.warn("doListSchemaNames: Unable to retrieve schemas from AWSGlue: {}", e.getMessage(), e);
        }

        if ((envVarService.isActivateLarkBaseSource() || envVarService.isActivateLarkDriveSource()) && !mappingTableDirectInitialized.isEmpty()) {
            if (envVarService.isEnableDebugLogging()) {
                logger.info("doListSchemaNames: Attempting to retrieve schemas from Lark Base.");
            }
            for (TableDirectInitialized entry : mappingTableDirectInitialized) {
                AthenaLarkBaseMapping dbMapping = entry.database();

                if (schemas.stream().anyMatch(database -> database.equalsIgnoreCase(dbMapping.athenaName()))) {
                    if (envVarService.isEnableDebugLogging()) {
                        logger.info("Database name {} is exist in glue, skipping", dbMapping.athenaName());
                    }
                    continue;
                }

                schemas.add(dbMapping.athenaName());
            }
        }

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Used to get a paginated list of tables that this source contains within a specific schema (database).
     * It retrieves tables primarily from AWS Glue.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog and database they are querying.
     * May include a nextToken for pagination.
     * @return A ListTablesResponse which primarily contains a List<TableName> enumerating the tables in this
     * catalog, database tuple. It also contains the catalog name corresponding the Athena catalog that was queried,
     * and potentially a nextToken if more tables exist.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) throws Exception
    {
        String requestedSchema = request.getSchemaName();

        if (envVarService.isEnableDebugLogging()) {
            logger.info("doListTables: enter - schema: {}}", requestedSchema);
        }

        Set<TableName> combinedTables = new LinkedHashSet<>();

        // 1. Take ALL tables from Glue ONLY on the first call
        try {
            // does not validate that the tables are actually DDB tables
            combinedTables.addAll(super.doListTables(allocator, new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(),
                    request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE), TABLE_FILTER).getTables());
        }
        catch (RuntimeException e) {
            logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
        }

        // 2. Always add tables from Lark mapping if the feature is active
        // (This will add the same tables again if they are in Glue, but Set will handle duplicates)
        if (envVarService.isActivateLarkBaseSource() || envVarService.isActivateLarkDriveSource()) {
            if (envVarService.isEnableDebugLogging()) {
                logger.info("doListTables: Checking Lark Base source mapping. envVarService.isActivateLarkBaseSource()={}, mappingTableDirectInitialized empty={}",
                        true, mappingTableDirectInitialized.isEmpty());
            }
            if (!mappingTableDirectInitialized.isEmpty()) {
                int larkTablesAddedCount = 0;
                for (TableDirectInitialized entry : mappingTableDirectInitialized) {
                    AthenaLarkBaseMapping dbMapping = entry.database();
                    if (dbMapping.athenaName().equalsIgnoreCase(requestedSchema)) {
                        if (envVarService.isEnableDebugLogging()) {
                            logger.info("doListTables: Found matching schema in mapping: {}", dbMapping.athenaName());
                        }
                        AthenaLarkBaseMapping tableMapping = entry.table();
                        TableName tableName = new TableName(dbMapping.athenaName(), tableMapping.athenaName());
                        boolean added = combinedTables.add(tableName);
                        if (added) {
                            larkTablesAddedCount++;
                            if (envVarService.isEnableDebugLogging()) {
                                logger.info("doListTables: Added table from mapping: {}", tableName);
                            }
                        }
                    }
                }
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("doListTables: Finished checking Lark mapping. Added {} unique tables from mapping for schema {}.", larkTablesAddedCount, requestedSchema);
                }
            }
            else {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("doListTables: Lark mapping is empty. No tables added from mapping.");
                }
            }
        }
        else {
            if (envVarService.isEnableDebugLogging()) {
                logger.info("doListTables: Lark Base source feature is not active (envVarService.isActivateLarkBaseSource()=false).");
            }
        }

        if (envVarService.isEnableDebugLogging()) {
            logger.info("doListTables: exit - returning {} tables for schema {}",
                    combinedTables.size(), requestedSchema);
        }

        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), null);
    }

    /**
     * Used to get the definition (field names, types, descriptions, etc...) of a specific Table.
     * This method retrieves the base schema from AWS Glue and enhances it by adding connector-specific
     * reserved columns like $reserved_record_id, $reserved_base_id, and $reserved_table_id.
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns (including added reserved columns), types, and descriptions.
     * 2. A Set<String> of partition column names (expected to be empty for Lark Base tables).
     * 3. A TableName object confirming the schema and table name the response is for.
     * 4. A catalog name corresponding the Athena catalog that was queried.
     * @throws RuntimeException if unable to retrieve the table definition from Glue.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        if (envVarService.isEnableDebugLogging()) {
            logger.info("doGetTable: Using Strategy Pattern for table {}", request.getTableName());
        }

        if (envVarService.isActivateLarkBaseSource() || envVarService.isActivateLarkDriveSource()) {
            if (envVarService.isEnableDebugLogging()) {
                logger.info("doGetTable: Attempting to get schema from Lark Base source.");
            }
            Optional<TableSchemaResult> schemaResult = larkSourceMetadataProvider.getTableSchema(request);

            if (schemaResult.isPresent()) {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("doGetTable: Found schema from Lark Base source.");
                }
                TableSchemaResult result = schemaResult.get();
                Schema finalSchema = CommonUtil.addReservedFields(result.schema());
                return new GetTableResponse(request.getCatalogName(), request.getTableName(), finalSchema, result.partitionColumns());
            }
        }

        if (envVarService.isActivateExperimentalFeatures()) {
            if (envVarService.isEnableDebugLogging()) {
                logger.info("doGetTable: Experimental feature is not active (envVarService.isActivateExperimentalFeatures()=false).");
            }
            Optional<TableSchemaResult> schemaResult = experimentalMetadataProvider.getTableSchema(request);
            if (schemaResult.isPresent()) {
                if (envVarService.isEnableDebugLogging()) {
                    logger.info("doGetTable: Found schema from experimental path.");
                }
                TableSchemaResult result = schemaResult.get();
                Schema finalSchema = CommonUtil.addReservedFields(result.schema());
                return new GetTableResponse(request.getCatalogName(), request.getTableName(), finalSchema, result.partitionColumns());
            }
        }

        try {
            GetTableResponse glueResponse = super.doGetTable(allocator, request);
            if (glueResponse != null && glueResponse.getSchema() != null) {
                logger.info("doGetTable: Found schema from Glue.");
                Schema finalSchema = CommonUtil.addReservedFields(glueResponse.getSchema());
                return new GetTableResponse(request.getCatalogName(), request.getTableName(), finalSchema, glueResponse.getPartitionColumns());
            }
            else {
                logger.warn("doGetTable: Glue fallback returned null or no schema for {}.", request.getTableName());
            }
        }
        catch (EntityNotFoundException e) {
            logger.warn("doGetTable: Glue fallback: Table {} not found in Glue.", request.getTableName());
        }
        catch (Exception e) {
            logger.warn("doGetTable: Error during Glue fallback for {}: {}", request.getTableName(), e.getMessage(), e);
        }

        logger.error("doGetTable: No schema found for {}. Returning empty schema.", request.getTableName());
        throw new RuntimeException("Unable to retrieve table schema from Glue or Lark Base source.");
    }

    /**
     * Defines the structure of the partition rows, including custom fields needed
     * to pass state (like Base ID, Table ID, translated filters) to doGetSplits.
     * This method is called by the default doGetTableLayout implementation.
     *
     * @param partitionSchemaBuilder The SchemaBuilder to add fields to.
     * @param request The GetTableLayoutRequest.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        logger.info("enhancePartitionSchema: Defining partition schema for {}", request.getTableName());
        partitionSchemaBuilder
                .addStringField(BASE_ID_PROPERTY)
                .addStringField(TABLE_ID_PROPERTY)
                .addStringField(FILTER_EXPRESSION_PROPERTY)
                .addIntField(PAGE_SIZE_PROPERTY)
                .addIntField(EXPECTED_ROW_COUNT_PROPERTY)
                .addStringField(SORT_EXPRESSION_PROPERTY)

                // Split Property
                .addBitField(IS_PARALLEL_SPLIT_PROPERTY)
                .addBigIntField(SPLIT_START_INDEX_PROPERTY)
                .addBigIntField(SPLIT_END_INDEX_PROPERTY)
                .addStringField(LARK_FIELD_TYPE_MAPPING_PROPERTY);
    }

    /**
     * Resolves partition information from various providers (Lark source, experimental, or Glue fallback).
     */
    private Optional<PartitionInfoResult> resolvePartitionInfo(TableName tableName, GetTableLayoutRequest request)
    {
        if (envVarService.isActivateLarkBaseSource() || envVarService.isActivateLarkDriveSource()) {
            logger.info("getPartitions: Attempting to get partition info from Lark Base source.");
            Optional<PartitionInfoResult> larkSourcePartitionInfo = larkSourceMetadataProvider.getPartitionInfo(tableName);
            if (larkSourcePartitionInfo.isPresent()) {
                logger.info("getPartitions: Found partition info from Lark Base source.");
                return larkSourcePartitionInfo;
            }
        }

        if (envVarService.isActivateLarkBaseSource()) {
            logger.info("getPartitions: Attempting to get partition info from experimental path.");
            Optional<PartitionInfoResult> experimentalPartitionInfo = experimentalMetadataProvider.getPartitionInfo(tableName, request);
            if (experimentalPartitionInfo.isPresent()) {
                logger.info("getPartitions: Found partition info from experimental path.");
                return experimentalPartitionInfo;
            }
        }

        logger.info("getPartitions: No specific provider found partition info, attempting Glue fallback directly...");
        try {
            Pair<String, String> ids = glueCatalogService.getLarkBaseAndTableIdFromTable(tableName.getSchemaName(), tableName.getTableName());
            if (ids != null && ids.left() != null && ids.right() != null) {
                List<AthenaFieldLarkBaseMapping> mappings = glueCatalogService.getFieldNameMappings(tableName.getSchemaName(), tableName.getTableName());
                return Optional.of(new PartitionInfoResult(ids.left(), ids.right(), mappings));
            }
        }
        catch (EntityNotFoundException e) {
            logger.warn("getPartitions: Glue Fallback: Table {} not found in Glue.", tableName);
        }
        catch (Exception e) {
            logger.warn("getPartitions: Error during Glue fallback for {}: {}", tableName, e.getMessage(), e);
        }

        return Optional.empty();
    }

    private long extractQueryLimit(GetTableLayoutRequest request)
    {
        if (request.getConstraints() != null && request.getConstraints().hasLimit()) {
            long limit = request.getConstraints().getLimit();
            logger.info("getPartitions: Query has LIMIT {}", limit);
            return limit;
        }
        return -1;
    }

    private String buildFieldTypeMappingJson(List<AthenaFieldLarkBaseMapping> fieldNameMappings, TableName tableName)
    {
        if (fieldNameMappings == null || fieldNameMappings.isEmpty()) {
            return "";
        }

        Map<String, NestedUIType> larkFieldTypeMap = new HashMap<>();
        for (AthenaFieldLarkBaseMapping mapping : fieldNameMappings) {
            larkFieldTypeMap.put(mapping.athenaName(), mapping.nestedUIType());
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(larkFieldTypeMap);
        }
        catch (JsonProcessingException e) {
            logger.warn("getPartitions: Failed to serialize Lark field type mapping for {}.{}: {}",
                    tableName.getSchemaName(), tableName.getTableName(), e.getMessage());
            return "";
        }
    }

    private String translateFilterExpression(GetTableLayoutRequest request, List<AthenaFieldLarkBaseMapping> fieldNameMappings, TableName tableName)
    {
        if (request.getConstraints() == null || request.getConstraints().getSummary() == null || request.getConstraints().getSummary().isEmpty()) {
            logger.info("getPartitions: No constraints to translate for {}", tableName);
            return "";
        }

        if (fieldNameMappings == null || fieldNameMappings.isEmpty()) {
            return "";
        }

        try {
            String filterExpression = SearchApiFilterTranslator.toFilterJson(request.getConstraints().getSummary(), fieldNameMappings);
            logger.info("getPartitions: Translated filter constraints for {}: {}", tableName, filterExpression);
            return filterExpression;
        }
        catch (Exception e) {
            logger.warn("getPartitions: Failed to translate filter constraints for {}: {}. Proceeding with empty filter.", tableName, e.getMessage(), e);
            return "";
        }
    }

    private boolean hasParallelSplitKey(List<AthenaFieldLarkBaseMapping> fieldNameMappings)
    {
        if (fieldNameMappings == null) {
            return false;
        }
        return fieldNameMappings.stream()
                .anyMatch(m -> RESERVED_SPLIT_KEY.equalsIgnoreCase(m.athenaName()));
    }

    private boolean hasOrderByClause(GetTableLayoutRequest request)
    {
        return request.getConstraints() != null &&
                request.getConstraints().getOrderByClause() != null &&
                !request.getConstraints().getOrderByClause().isEmpty();
    }

    private String translateSortExpression(GetTableLayoutRequest request, List<AthenaFieldLarkBaseMapping> fieldNameMappings,
                                           boolean useParallelSplits, boolean hasOrderBy, TableName tableName)
    {
        if (fieldNameMappings == null || useParallelSplits || !hasOrderBy || fieldNameMappings.isEmpty()) {
            return "";
        }

        try {
            return SearchApiFilterTranslator.toSortJson(request.getConstraints().getOrderByClause(), fieldNameMappings);
        }
        catch (Exception e) {
            logger.warn("getPartitions: Failed to translate sort expression for {}: {}. Proceeding without sort.", tableName, e.getMessage(), e);
            return "";
        }
    }

    private long calculateEffectiveRowCount(long totalRowCount, long queryLimit, boolean hasOrderBy)
    {
        if (hasOrderBy) {
            logger.info("getPartitions: ORDER BY detected. Ignoring LIMIT {}. Effective count: {}", queryLimit, totalRowCount);
            return totalRowCount;
        }

        if (queryLimit >= 0 && queryLimit < totalRowCount) {
            logger.info("getPartitions: Applying LIMIT. Effective row count: {}", queryLimit);
            return queryLimit;
        }

        logger.info("getPartitions: No LIMIT or LIMIT exceeds total. Effective row count: {}", totalRowCount);
        return totalRowCount;
    }

    private void writeParallelPartitions(BlockWriter blockWriter, String baseId, String tableId,
                                         String filterExpression, String fieldTypeMappingJson,
                                         long queryLimit, boolean hasOrderBy)
    {
        int totalRowCount = getTotalRowCount(baseId, tableId, null);
        long effectiveRowCount = calculateEffectiveRowCount(totalRowCount, queryLimit, hasOrderBy);

        if (effectiveRowCount == 0 && totalRowCount > 0) {
            logger.info("getPartitions: Effective row count is 0 due to LIMIT, writing no partitions.");
            return;
        }

        int numSplits = (int) Math.ceil((double) effectiveRowCount / PAGE_SIZE);
        logger.info("getPartitions: Writing {} parallel partition rows for {} effective rows.", numSplits, effectiveRowCount);

        for (int i = 0; i < numSplits; i++) {
            final long startIndex = (long) i * PAGE_SIZE + 1;
            final long endIndex = Math.min((long) (i + 1) * PAGE_SIZE, effectiveRowCount);
            final long currentSplitRowCount = endIndex - startIndex + 1;

            blockWriter.writeRows((block, rowNum) -> {
                BlockUtils.setValue(block.getFieldVector(BASE_ID_PROPERTY), rowNum, baseId);
                BlockUtils.setValue(block.getFieldVector(TABLE_ID_PROPERTY), rowNum, tableId);
                BlockUtils.setValue(block.getFieldVector(FILTER_EXPRESSION_PROPERTY), rowNum, filterExpression);
                BlockUtils.setValue(block.getFieldVector(SORT_EXPRESSION_PROPERTY), rowNum, "");
                BlockUtils.setValue(block.getFieldVector(PAGE_SIZE_PROPERTY), rowNum, PAGE_SIZE);
                BlockUtils.setValue(block.getFieldVector(EXPECTED_ROW_COUNT_PROPERTY), rowNum, (int) currentSplitRowCount);
                BlockUtils.setValue(block.getFieldVector(IS_PARALLEL_SPLIT_PROPERTY), rowNum, true);
                BlockUtils.setValue(block.getFieldVector(SPLIT_START_INDEX_PROPERTY), rowNum, startIndex);
                BlockUtils.setValue(block.getFieldVector(SPLIT_END_INDEX_PROPERTY), rowNum, endIndex);
                BlockUtils.setValue(block.getFieldVector(LARK_FIELD_TYPE_MAPPING_PROPERTY), rowNum, fieldTypeMappingJson);
                return 1;
            });
        }
        logger.info("getPartitions: Successfully wrote {} parallel partition rows.", numSplits);
    }

    private void writeSinglePartition(BlockWriter blockWriter, String baseId, String tableId,
                                      String filterExpression, String sortExpression, String fieldTypeMappingJson,
                                      long queryLimit, boolean useParallelSplits, boolean hasOrderBy)
    {
        int totalRowCount = getTotalRowCount(baseId, tableId, filterExpression);
        long effectiveRowCount = calculateEffectiveRowCount(totalRowCount, queryLimit, useParallelSplits && hasOrderBy);

        if (effectiveRowCount == 0 && totalRowCount > 0) {
            logger.info("getPartitions: Effective row count is 0 due to LIMIT, writing no partitions.");
            return;
        }

        logger.info("getPartitions: Writing 1 single partition row.");
        final int finalExpectedRowCount = (int) effectiveRowCount;

        blockWriter.writeRows((block, rowNum) -> {
            BlockUtils.setValue(block.getFieldVector(BASE_ID_PROPERTY), rowNum, baseId);
            BlockUtils.setValue(block.getFieldVector(TABLE_ID_PROPERTY), rowNum, tableId);
            BlockUtils.setValue(block.getFieldVector(FILTER_EXPRESSION_PROPERTY), rowNum, filterExpression);
            BlockUtils.setValue(block.getFieldVector(SORT_EXPRESSION_PROPERTY), rowNum, sortExpression);
            BlockUtils.setValue(block.getFieldVector(PAGE_SIZE_PROPERTY), rowNum, PAGE_SIZE);
            BlockUtils.setValue(block.getFieldVector(EXPECTED_ROW_COUNT_PROPERTY), rowNum, finalExpectedRowCount);
            BlockUtils.setValue(block.getFieldVector(IS_PARALLEL_SPLIT_PROPERTY), rowNum, false);
            BlockUtils.setValue(block.getFieldVector(SPLIT_START_INDEX_PROPERTY), rowNum, 0L);
            BlockUtils.setValue(block.getFieldVector(SPLIT_END_INDEX_PROPERTY), rowNum, 0L);
            BlockUtils.setValue(block.getFieldVector(LARK_FIELD_TYPE_MAPPING_PROPERTY), rowNum, fieldTypeMappingJson);
            return 1;
        });
        logger.info("getPartitions: Successfully wrote 1 single partition row.");
    }

    private int getTotalRowCount(String baseId, String tableId, String filterExpression)
    {
        try {
            com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest request =
                    com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest.builder()
                            .baseId(baseId)
                            .tableId(tableId)
                            .pageSize(1)
                            .filterJson(filterExpression)
                            .sortJson("")
                            .build();

            int total = invoker.invoke(() -> larkBaseService.getTableRecords(request)).getTotal();
            logger.info("getPartitions: Estimated total row count matching filter: {}", total);
            return total;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to estimate row count for partition planning.", e);
        }
    }

    /**
     * Determines the partition details for the request. For Lark Base, this involves
     * finding the correct Base ID and Table ID, translating constraints and ordering,
     * estimating row counts, and writing a SINGLE partition row containing this state
     * to the BlockWriter. This method implements the logic previously found in the
     * overridden doGetTableLayout.
     *
     * @param blockWriter Used to write the single partition row.
     * @param request Provides details on the table being queried, constraints, and the query ID.
     * @param queryStatusChecker Allows checking if the query is still active.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
    {
        requireNonNull(blockWriter, "blockWriter cannot be null");
        requireNonNull(request, "request cannot be null");
        requireNonNull(queryStatusChecker, "queryStatusChecker cannot be null");

        logger.info("getPartitions: Using Strategy Pattern for table {}", request.getTableName());
        TableName tableName = request.getTableName();

        Optional<PartitionInfoResult> partitionInfoOpt = resolvePartitionInfo(tableName, request);
        if (partitionInfoOpt.isEmpty()) {
            logger.error("getPartitions: Could not determine BaseID and TableID for table {} using any provider. No partitions written.", tableName);
            return;
        }

        PartitionInfoResult partitionInfo = partitionInfoOpt.get();
        final String baseId = partitionInfo.baseId();
        final String tableId = partitionInfo.tableId();
        final List<AthenaFieldLarkBaseMapping> fieldNameMappings = partitionInfo.fieldNameMappings();

        if (baseId == null || tableId == null || baseId.isEmpty() || tableId.isEmpty()) {
            logger.error("getPartitions: Invalid BaseID or TableID. No partitions written.");
            return;
        }

        long queryLimit = extractQueryLimit(request);
        String fieldTypeMappingJson = buildFieldTypeMappingJson(fieldNameMappings, tableName);
        String filterExpression = translateFilterExpression(request, fieldNameMappings, tableName);

        boolean useParallelSplits = hasParallelSplitKey(fieldNameMappings);
        boolean hasOrderBy = hasOrderByClause(request);
        String sortExpression = translateSortExpression(request, fieldNameMappings, useParallelSplits, hasOrderBy, tableName);

        if (useParallelSplits && envVarService.isActivateParallelSplit()) {
            writeParallelPartitions(blockWriter, baseId, tableId, filterExpression, fieldTypeMappingJson,
                    queryLimit, hasOrderBy);
        }
        else {
            writeSinglePartition(blockWriter, baseId, tableId, filterExpression, sortExpression,
                    fieldTypeMappingJson, queryLimit, useParallelSplits, hasOrderBy);
        }
    }

    /**
     * Generates splits for the requested partition(s). Following the single-partition strategy
     * implemented in `getPartitions`, this method expects exactly one partition row in the request.
     * It reads the properties (filter, page size, page token, base ID, table ID) from that single
     * partition and creates **exactly one Split**. This Split contains all the information the
     * `RecordHandler` needs to start reading data sequentially from the Lark Base table, beginning
     * from the first page (as the initial page token is empty).
     *
     * @param allocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details of the catalog, database, table, and the single partition generated
     * by `getPartitions`.
     * @return A GetSplitsResponse containing:
     * 1. A Set containing the single Split representing the entire read operation for the table/query.
     * 2. A null continuation token (as all splits are generated at once).
     * @throws RuntimeException if the number of partitions received is not exactly one.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        TableName tableName = request.getTableName();
        logger.info("doGetSplits: enter for table {}", tableName);

        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        int partitionCount = partitions.getRowCount();

        if (partitionCount == 0) {
            logger.error("No partition rows found for table {}. Cannot create splits.", tableName);
            return new GetSplitsResponse(request.getCatalogName(), Collections.emptySet());
        }

        logger.info("doGetSplits: Received {} partition row(s) for table {}", partitionCount, tableName);

        FieldReader baseIdReader = partitions.getFieldReader(BASE_ID_PROPERTY);
        FieldReader tableIdReader = partitions.getFieldReader(TABLE_ID_PROPERTY);
        FieldReader filterExprReader = partitions.getFieldReader(FILTER_EXPRESSION_PROPERTY);
        FieldReader sortExprReader = partitions.getFieldReader(SORT_EXPRESSION_PROPERTY);
        FieldReader pageSizeReader = partitions.getFieldReader(PAGE_SIZE_PROPERTY);
        FieldReader expectedCountReader = partitions.getFieldReader(EXPECTED_ROW_COUNT_PROPERTY);
        FieldReader isParallelReader = partitions.getFieldReader(IS_PARALLEL_SPLIT_PROPERTY);
        FieldReader startIndexReader = partitions.getFieldReader(SPLIT_START_INDEX_PROPERTY);
        FieldReader endIndexReader = partitions.getFieldReader(SPLIT_END_INDEX_PROPERTY);
        FieldReader larkFieldTypeMappingReader = partitions.getFieldReader(LARK_FIELD_TYPE_MAPPING_PROPERTY);

        for (int rowNum = 0; rowNum < partitionCount; rowNum++) {
            logger.debug("doGetSplits: Processing partition row {}", rowNum);

            String baseId = FieldReaderUtil.readText(baseIdReader, rowNum);
            String tableId = FieldReaderUtil.readText(tableIdReader, rowNum);
            String filterExpression = FieldReaderUtil.readText(filterExprReader, rowNum);
            String sortExpression = FieldReaderUtil.readText(sortExprReader, rowNum);
            int pageSizeFromPartition = FieldReaderUtil.readInt(pageSizeReader, rowNum);
            long expectedRowCountFromPartition = FieldReaderUtil.readInt(expectedCountReader, rowNum);
            boolean isParallelSplit = FieldReaderUtil.readBoolean(isParallelReader, rowNum);
            long splitStartIndex = FieldReaderUtil.readLong(startIndexReader, rowNum);
            long splitEndIndex = FieldReaderUtil.readLong(endIndexReader, rowNum);
            String larkFieldTypeMappingJson = FieldReaderUtil.readText(larkFieldTypeMappingReader, rowNum);

            int pageSizeForSplit = pageSizeFromPartition;
            long finalExpectedRowCount = expectedRowCountFromPartition;

            if (request.getConstraints().hasLimit()) {
                long limit = request.getConstraints().getLimit();
                if (limit > 0 && limit < PAGE_SIZE) {
                    int intLimit = (int) limit;
                    if (intLimit < pageSizeForSplit) {
                        pageSizeForSplit = intLimit;
                        logger.info("doGetSplits: Applying LIMIT {} as page size for split row {} (Top-N optimization)", pageSizeForSplit, rowNum);
                    }
                }
                if (!isParallelSplit && finalExpectedRowCount > limit && limit >= 0) {
                    finalExpectedRowCount = limit;
                }
            }

            Split.Builder splitBuilder = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(BASE_ID_PROPERTY, baseId)
                    .add(TABLE_ID_PROPERTY, tableId)
                    .add(FILTER_EXPRESSION_PROPERTY, filterExpression)
                    .add(PAGE_SIZE_PROPERTY, String.valueOf(pageSizeForSplit))
                    .add(EXPECTED_ROW_COUNT_PROPERTY, String.valueOf(finalExpectedRowCount))
                    .add(IS_PARALLEL_SPLIT_PROPERTY, String.valueOf(isParallelSplit))
                    .add(SPLIT_START_INDEX_PROPERTY, String.valueOf(splitStartIndex))
                    .add(SPLIT_END_INDEX_PROPERTY, String.valueOf(splitEndIndex))
                    .add(LARK_FIELD_TYPE_MAPPING_PROPERTY, larkFieldTypeMappingJson);

            if (!isParallelSplit && !sortExpression.isEmpty()) {
                splitBuilder.add(SORT_EXPRESSION_PROPERTY, sortExpression);
            }

            splits.add(splitBuilder.build());
            logger.debug("doGetSplits: Created split for partition row {}: Parallel={}, Range={}-{}, PageSize={}, ExpectedRows={}",
                    rowNum, isParallelSplit, splitStartIndex, splitEndIndex, pageSizeForSplit, finalExpectedRowCount);
        }

        logger.info("doGetSplits: Finished. Returning {} splits for table {}", splits.size(), request.getTableName());
        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    private static class FieldReaderUtil
    {
        static String readText(FieldReader reader, int position)
        {
            if (reader == null) {
                return "";
            }
            reader.setPosition(position);
            return reader.isSet() ? reader.readText().toString() : "";
        }

        static int readInt(FieldReader reader, int position)
        {
            if (reader == null) {
                return 0;
            }
            reader.setPosition(position);
            return reader.isSet() ? (reader.readInteger() != null ? reader.readInteger() : 0) : 0;
        }

        static long readLong(FieldReader reader, int position)
        {
            if (reader == null) {
                return 0L;
            }
            reader.setPosition(position);
            return reader.isSet() ? (reader.readLong() != null ? reader.readLong() : 0L) : 0L;
        }

        static boolean readBoolean(FieldReader reader, int position)
        {
            if (reader == null) {
                return false;
            }
            reader.setPosition(position);
            return reader.isSet() && (reader.readBoolean() != null ? reader.readBoolean() : false);
        }
    }

    /**
     * Defines the optimization capabilities that this connector supports for query execution.
     * This method informs the Athena query engine about which parts of query processing can be
     * pushed down to the underlying data source (Lark Bitable API), significantly improving
     * query performance and reducing data transfer.
     * <p>
     * The optimizations enabled in this implementation are:
     * <p>
     * 1. LIMIT Pushdown:
     * - Capability: Pushes the LIMIT clause from Athena queries to the Lark Bitable API.
     * - Benefit: Only the approximate number of records needed might be fetched initially (controlled by page size),
     * and Athena stops requesting more data once the limit is reached. While not a direct translation of LIMIT N
     * to the API, declaring support allows Athena to optimize by stopping early.
     * - Implementation: Primarily relies on Athena stopping calls to the RecordHandler. The connector itself uses 'page_size'.
     * - Example: "SELECT * FROM table LIMIT 100" - Athena will stop asking the RecordHandler for data after 100 records.
     * <p>
     * 2. TOP N Pushdown:
     * - Capability: Pushes ORDER BY + LIMIT operations to the data source.
     * - Benefit: Sorting happens at the data source, eliminating the need for Athena to
     * fetch the entire dataset and sort it.
     * - Implementation: Combines 'sort' and 'page_size' parameters in the Lark Bitable API.
     * - Example: "SELECT * FROM table ORDER BY column DESC LIMIT 10" will request the top 10
     * records, already sorted, from the API (within the first page).
     * <p>
     * 3. Filter Pushdown:
     * - Capability: Pushes WHERE clause predicates to the data source.
     * - Benefit: Data filtering happens at the source, dramatically reducing the volume of
     * data transferred to Athena.
     * - Implementation: Translates WHERE clauses to the 'filter' parameter in the Lark Bitable API.
     * - Supported filter types: EQUATABLE_VALUE_SET, SORTED_RANGE_SET, ALL_OR_NONE_VALUE_SET, NULLABLE_COMPARISON.
     * - Example: "SELECT * FROM table WHERE status = 'active' AND created_date > '2023-01-01'" will filter records at the API level.
     * - Unsupported filters: LIKE, REGEX, and other complex expressions.
     * <p>
     * When a query is executed, Athena uses these capabilities to optimize the execution plan,
     * pushing as much of the query processing as possible to the Lark Bitable API, reducing
     * the amount of data transferred and processed in Athena itself.
     *
     * @param allocator BlockAllocator used to allocate Apache Arrow memory for the request/response.
     * @param request Contains metadata about the request including the catalog name.
     * @return A response containing the data source capabilities.
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        Map<String, List<OptimizationSubType>> capabilities = new HashMap<>();

        // Enable LIMIT pushdown - translates LIMIT clauses to page_size parameter
        // This allows Athena to request only the number of records needed
        Map.Entry<String, List<OptimizationSubType>> limitPushdownCapability =
                DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                        LimitPushdownSubType.INTEGER_CONSTANT
                );
        capabilities.put(limitPushdownCapability.getKey(), limitPushdownCapability.getValue());

        // Enable TOP N pushdown - combines ORDER BY and LIMIT operations
        // This allows sorting to occur at the data source before returning results
        // Take a note that only ORDER BY + LIMIT that will trigger this optimization, not without LIMIT
        Map.Entry<String, List<OptimizationSubType>> topNPushdownCapability =
                DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                        TopNPushdownSubType.SUPPORTS_ORDER_BY
                );
        capabilities.put(topNPushdownCapability.getKey(), topNPushdownCapability.getValue());

        // Enable filter pushdown - translates WHERE clauses to filter parameter
        // This allows filtering to happen at the data source level
        Map.Entry<String, List<OptimizationSubType>> filterPushdownCapability =
                DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                        // Support for basic comparison operators (=, !=, >, <, >= , <= )
                        FilterPushdownSubType.EQUATABLE_VALUE_SET,
                        // Support for range filters like BETWEEN or combined conditions
                        FilterPushdownSubType.SORTED_RANGE_SET,
                        // Support for IN and NOT IN operators with value lists
                        FilterPushdownSubType.ALL_OR_NONE_VALUE_SET,
                        // Support for NULL checks (IS NULL, IS NOT NULL)
                        FilterPushdownSubType.NULLABLE_COMPARISON
                );
        capabilities.put(filterPushdownCapability.getKey(), filterPushdownCapability.getValue());

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities);
    }
}
