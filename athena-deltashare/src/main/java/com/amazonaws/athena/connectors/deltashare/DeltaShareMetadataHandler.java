/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
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
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connectors.deltashare.client.DeltaShareClient;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import com.amazonaws.athena.connectors.deltashare.util.DeltaSharePredicateUtils;
import com.amazonaws.athena.connectors.deltashare.util.DeltaShareSchemaBuilder;
import com.amazonaws.athena.connectors.deltashare.util.ParquetReaderUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Handles metadata for Delta Share. Supports predicate pushdown and row group partitioning for large files.
 */
public class DeltaShareMetadataHandler extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaShareMetadataHandler.class);
    private static final String SOURCE_TYPE = "deltashare";
    private static final int MAX_SPLITS_PER_REQUEST = 1000;
    
    
    private static final String PARTITION_ID_METADATA = "partition_id";
    private static final String JSON_PREDICATE_HINTS_METADATA = "json_predicate_hints";
    private static final String PARTITION_VALUES_METADATA = "partition_values";
    private static final String FILE_COUNT_METADATA = "file_count";
    private static final String SHARE_METADATA = "share";
    private static final String SCHEMA_METADATA = "schema";
    private static final String TABLE_METADATA = "table";
    
    private static final String PRESIGNED_URL_METADATA = "presigned_url";
    private static final String ROW_GROUP_INDEX_METADATA = "row_group_index";
    private static final String PROCESSING_MODE_METADATA = "processing_mode";
    private static final String FILE_SIZE_METADATA = "file_size";
    private static final String TOTAL_ROW_GROUPS_METADATA = "total_row_groups";

    private final DeltaShareClient deltaShareClient;
    private final ObjectMapper objectMapper;
    private final String configuredShareName;

    public DeltaShareMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        String endpoint = configOptions.get(DeltaShareConstants.ENDPOINT_PROPERTY);
        String token = configOptions.get(DeltaShareConstants.TOKEN_PROPERTY);
        this.configuredShareName = configOptions.get(DeltaShareConstants.SHARE_NAME_PROPERTY);
        this.deltaShareClient = new DeltaShareClient(endpoint, token);
        this.objectMapper = new ObjectMapper();
        
        logger.info("DeltaShareMetadataHandler initialized for share: {}", configuredShareName);
    }

    protected DeltaShareMetadataHandler(
            EncryptionKeyFactory keyFactory,
            SecretsManagerClient awsSecretsManager,
            AthenaClient athena,
            String spillBucket,
            String spillPrefix,
            java.util.Map<String, String> configOptions)
    {
        super(keyFactory, awsSecretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        String endpoint = configOptions.get(DeltaShareConstants.ENDPOINT_PROPERTY);
        String token = configOptions.get(DeltaShareConstants.TOKEN_PROPERTY);
        this.configuredShareName = configOptions.get(DeltaShareConstants.SHARE_NAME_PROPERTY);
        this.deltaShareClient = new DeltaShareClient(endpoint, token);
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Lists the schemas available through Delta Share.
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        Set<String> schemas = new HashSet<>();
        
        try
        {
            if (configuredShareName == null || configuredShareName.isEmpty())
            {
                throw new RuntimeException("share_name must be configured in environment variables");
            }
            
            List<String> schemasInShare = deltaShareClient.listSchemas(configuredShareName);
            schemas.addAll(schemasInShare);
            
            if (schemas.isEmpty())
            {
                logger.warn("No schemas found in share '{}', adding default", configuredShareName);
                schemas.add("default");
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to list schemas from Delta Share: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to list schemas from share '" + configuredShareName + "': " + e.getMessage(), e);
        }

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Lists the tables available in the specified schema.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        List<TableName> tables = new ArrayList<>();
        String schemaName = request.getSchemaName();
        
        try
        {
            if (configuredShareName == null || configuredShareName.isEmpty())
            {
                throw new RuntimeException("share_name must be configured in environment variables");
            }
            
            List<com.amazonaws.athena.connectors.deltashare.model.DeltaShareTable> deltaShareTables = deltaShareClient.listTables(configuredShareName, schemaName);
            
            for (com.amazonaws.athena.connectors.deltashare.model.DeltaShareTable deltaTable : deltaShareTables)
            {
                tables.add(new TableName(schemaName, deltaTable.getName()));
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to list tables for schema {}: {}", schemaName, e.getMessage(), e);
            throw new RuntimeException("Failed to list tables for schema '" + schemaName + "' in share '" + configuredShareName + "': " + e.getMessage(), e);
        }

        return new ListTablesResponse(request.getCatalogName(), tables, null);
    }

    /**
     * Gets the table definition from Delta Share and converts to Arrow format.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        String schemaName = request.getTableName().getSchemaName();
        String tableName = request.getTableName().getTableName();
        
        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();
        Set<String> partitionColNames = new HashSet<>();

        try
        {
            if (configuredShareName == null || configuredShareName.isEmpty())
            {
                throw new RuntimeException("share_name must be configured in environment variables");
            }
            
            JsonNode schemaRoot = deltaShareClient.getTableMetadata(configuredShareName, schemaName, tableName);
            
            if (schemaRoot == null) {
                throw new RuntimeException("No schema metadata returned from Delta Share for table: " + tableName);
            }
            
            if (schemaRoot.has("partitionColumns")) {
                JsonNode partitionCols = schemaRoot.get("partitionColumns");
                if (partitionCols.isArray()) {
                    for (JsonNode partitionCol : partitionCols) {
                        String partitionColName = partitionCol.asText();
                        partitionColNames.add(partitionColName);
                    }
                }
            }
            
            if (schemaRoot.has("fields"))
            {
                JsonNode fields = schemaRoot.get("fields");
                
                for (JsonNode field : fields)
                {
                    String fieldName = field.get("name").asText();
                    String fieldType = field.get("type").asText();
                    
                    convertAndAddField(tableSchemaBuilder, fieldName, fieldType, partitionColNames);
                    
                    if (field.has("metadata") && field.get("metadata").has("partitionIndex"))
                    {
                        partitionColNames.add(fieldName);
                    }
                }
            }
            
            for (String partitionCol : partitionColNames) {
                boolean fieldExists = false;
                if (schemaRoot.has("fields")) {
                    JsonNode fields = schemaRoot.get("fields");
                    for (JsonNode field : fields) {
                        if (field.get("name").asText().equals(partitionCol)) {
                            fieldExists = true;
                            break;
                        }
                    }
                }
                
                if (!fieldExists) {
                    ArrowType partitionColType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("string");
                    Field partitionField = new Field(partitionCol, org.apache.arrow.vector.types.pojo.FieldType.nullable(partitionColType), null);
                    tableSchemaBuilder.addField(partitionField);
                }
            }
            
            tableSchemaBuilder.addMetadata("source", "deltashare")
                             .addMetadata("share", configuredShareName)
                             .addMetadata("schema", schemaName)
                             .addMetadata("table", tableName);
                             
            if (!partitionColNames.isEmpty())
            {
                tableSchemaBuilder.addMetadata("partitionCols", String.join(",", partitionColNames));
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to get table schema for {}.{}: {}", schemaName, tableName, e.getMessage(), e);
            throw new RuntimeException("Failed to get table schema from Delta Share", e);
        }

        
        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }


    /**
     * Returns supported optimizations including filter pushdown and complex expressions.
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));
        
        List<StandardFunctions> supportedFunctions = Arrays.asList(
                StandardFunctions.AND_FUNCTION_NAME,
                StandardFunctions.OR_FUNCTION_NAME,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
                StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
                StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                StandardFunctions.IS_NULL_FUNCTION_NAME
        );
        
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(supportedFunctions.stream()
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Enhances partition schema with metadata for predicate hints and row group processing.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        TableName tableName = request.getTableName();
        Schema tableSchema = request.getSchema();
        String schemaName = tableName.getSchemaName();
        String table = tableName.getTableName();
        
        if (configuredShareName == null || configuredShareName.isEmpty())
        {
            throw new RuntimeException("share_name must be configured in environment variables");
        }
        
        partitionSchemaBuilder.addMetadata(SHARE_METADATA, configuredShareName);
        partitionSchemaBuilder.addMetadata(SCHEMA_METADATA, schemaName);
        partitionSchemaBuilder.addMetadata(TABLE_METADATA, table);
        
        List<String> partitionColumnNames = new ArrayList<>();
        
        if (request.getPartitionCols() != null && !request.getPartitionCols().isEmpty()) {
            for (String partitionCol : request.getPartitionCols()) {
                partitionColumnNames.add(partitionCol);
            }
        } else {
            String partitionCols = tableSchema.getCustomMetadata().get("partitionCols");
            if (partitionCols != null && !partitionCols.isEmpty()) {
                partitionColumnNames.addAll(Arrays.asList(partitionCols.split(",")));
            }
        }
        
        Map<String, ValueSet> summary = request.getConstraints().getSummary();
        QueryPlan queryPlan = request.getConstraints().getQueryPlan();
        
        boolean useQueryPlan = Objects.nonNull(queryPlan);
        Map<String, List<ColumnPredicate>> filterPredicates = new HashMap<>();
        String jsonPredicateHints = null;
        
        if (useQueryPlan) {
            try {
                Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
                filterPredicates = DeltaSharePredicateUtils.buildFilterPredicatesFromPlan(plan);
                jsonPredicateHints = DeltaSharePredicateUtils.buildJsonPredicateHints(filterPredicates, partitionColumnNames);
            } catch (Exception e) {
                logger.warn("Failed to process substrait plan, falling back to legacy constraints: {}", e.getMessage());
                useQueryPlan = false;
            }
        }
        
        if (!useQueryPlan) {
            jsonPredicateHints = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(summary, partitionColumnNames);
        }
        
        if (jsonPredicateHints != null && !jsonPredicateHints.isEmpty()) {
            partitionSchemaBuilder.addMetadata(JSON_PREDICATE_HINTS_METADATA, jsonPredicateHints);
        }
        
        for (String partitionCol : partitionColumnNames) {
            Field field = findFieldInSchema(tableSchema, partitionCol);
            if (field != null) {
                ArrowType fieldType = field.getType();
                
                if (fieldType instanceof ArrowType.Date || fieldType instanceof ArrowType.Timestamp) {
                    partitionSchemaBuilder.addStringField(partitionCol);
                } else {
                    partitionSchemaBuilder.addField(field);
                }
            } else {
                partitionSchemaBuilder.addStringField(partitionCol);
            }
        }
        
        partitionSchemaBuilder.addStringField(PARTITION_ID_METADATA);
        partitionSchemaBuilder.addIntField(FILE_COUNT_METADATA);
        
        partitionSchemaBuilder.addStringField(PRESIGNED_URL_METADATA);
        partitionSchemaBuilder.addIntField(ROW_GROUP_INDEX_METADATA);
        partitionSchemaBuilder.addStringField(PROCESSING_MODE_METADATA);
        partitionSchemaBuilder.addBigIntField(FILE_SIZE_METADATA);
        partitionSchemaBuilder.addIntField(TOTAL_ROW_GROUPS_METADATA);
        partitionSchemaBuilder.addIntField("split_order_priority");
        
    }

    /**
     * Converts Delta Lake field types to Arrow types. Partition date/timestamp columns are converted to strings.
     */
    private void convertAndAddField(SchemaBuilder builder, String fieldName, String deltaType, Set<String> partitionColumns)
    {
        try
        {
            boolean isPartitionColumn = partitionColumns.contains(fieldName);
            boolean isDateType = "date".equals(deltaType.toLowerCase()) || "timestamp".equals(deltaType.toLowerCase());
            
            if (isPartitionColumn && isDateType) {
                builder.addStringField(fieldName);
                return;
            }
            
            switch (deltaType.toLowerCase())
            {
                case "string":
                    builder.addStringField(fieldName);
                    break;
                case "integer":
                case "int":
                    builder.addIntField(fieldName);
                    break;
                case "long":
                case "bigint":
                    builder.addBigIntField(fieldName);
                    break;
                case "double":
                case "float":
                    builder.addFloat8Field(fieldName);
                    break;
                case "boolean":
                    builder.addBitField(fieldName);
                    break;
                case "date":
                    builder.addDateDayField(fieldName);
                    break;
                case "timestamp":
                    builder.addDateMilliField(fieldName);
                    break;
                default:
                    builder.addStringField(fieldName);
                    logger.warn("Unknown Delta type '{}' for field '{}', defaulting to string", deltaType, fieldName);
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to add field '{}' of type '{}', adding as string: {}", fieldName, deltaType, e.getMessage());
            builder.addStringField(fieldName);
        }
    }

    /**
     * Creates a partition key from file partition values.
     */
    private String getPartitionKey(JsonNode file)
    {
        if (file.has("partitionValues"))
        {
            JsonNode partitionValues = file.get("partitionValues");
            if (partitionValues.size() == 0)
            {
                return "no_partition";
            }
            
            StringBuilder keyBuilder = new StringBuilder();
            for (Iterator<String> fieldNames = partitionValues.fieldNames(); fieldNames.hasNext();)
            {
                String fieldName = fieldNames.next();
                JsonNode value = partitionValues.get(fieldName);
                if (keyBuilder.length() > 0)
                {
                    keyBuilder.append(",");
                }
                keyBuilder.append(fieldName).append("=").append(value.asText());
            }
            return keyBuilder.toString();
        }
        else
        {
            return "no_partition";
        }
    }
    
    
    /**
     * Finds a field in the table schema by name.
     */
    private Field findFieldInSchema(Schema schema, String fieldName)
    {
        for (Field field : schema.getFields()) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }
    
    /**
     * Creates partitions for Delta Share table. Large files are split into row group partitions for parallel processing.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        TableName tableName = request.getTableName();
        String schemaName = tableName.getSchemaName();
        String table = tableName.getTableName();
        
        if (configuredShareName == null || configuredShareName.isEmpty())
        {
            throw new RuntimeException("share_name must be configured in environment variables");
        }
        
        try {
            String jsonPredicateHints = null;
            
            SchemaBuilder tempBuilder = SchemaBuilder.newBuilder();
            enhancePartitionSchema(tempBuilder, request);
            Schema partitionSchema = tempBuilder.build();
            
            Map<String, String> partitionMetadata = partitionSchema.getCustomMetadata();
            if (partitionMetadata.containsKey(JSON_PREDICATE_HINTS_METADATA)) {
                jsonPredicateHints = partitionMetadata.get(JSON_PREDICATE_HINTS_METADATA);
            }
            
            JsonNode queryResult;
            if (jsonPredicateHints != null && !jsonPredicateHints.isEmpty()) {
                queryResult = deltaShareClient.queryTableWithPredicateHints(configuredShareName, schemaName, table, jsonPredicateHints);
            } else {
                queryResult = deltaShareClient.queryTable(configuredShareName, schemaName, table);
            }
            
            if (queryResult == null || !queryResult.isArray() || queryResult.size() == 0) {
                logger.warn("No files found for table {}.{}.{}", configuredShareName, schemaName, table);
                return;
            }
            
            Map<String, PartitionInfo> partitionGroups = new HashMap<>();
            
            
            for (int i = 0; i < queryResult.size(); i++) {
                JsonNode fileEntry = queryResult.get(i);
                
                if (fileEntry.has("protocol") || fileEntry.has("metaData")) {
                    continue;
                }
                
                JsonNode file = null;
                
                if (fileEntry.has("file")) {
                    file = fileEntry.get("file");
                } else if (fileEntry.has("url")) {
                    file = fileEntry;
                } else {
                    logger.warn("Unrecognized file entry structure at index {}", i);
                    continue;
                }
                
                long fileSize = file.has("size") ? file.get("size").asLong() : 0;
                String presignedUrl = file.has("url") ? file.get("url").asText() : null;
                
                if (fileSize > DeltaShareConstants.FILE_SIZE_THRESHOLD && presignedUrl != null) {
                    
                    try {
                        org.apache.parquet.hadoop.metadata.ParquetMetadata metadata = ParquetReaderUtil.getParquetMetadata(presignedUrl, fileSize);
                        List<org.apache.parquet.hadoop.metadata.BlockMetaData> actualRowGroups = metadata.getBlocks();
                        int actualRowGroupCount = actualRowGroups.size();
                        
                        long totalRows = actualRowGroups.stream().mapToLong(org.apache.parquet.hadoop.metadata.BlockMetaData::getRowCount).sum();
                        logger.info("Creating {} row group partitions for {}MB file ({} total rows)", 
                                   actualRowGroupCount, fileSize / (1024 * 1024), totalRows);
                        
                        for (int rowGroupIndex = 0; rowGroupIndex < actualRowGroupCount; rowGroupIndex++) {
                            String rowGroupPartitionKey = String.format("%s_rg_%03d", getPartitionKey(file), rowGroupIndex);
                            
                            writeRowGroupPartition(blockWriter, file, rowGroupPartitionKey, 
                                                 rowGroupIndex, actualRowGroupCount, fileSize, presignedUrl);
                        }
                        
                        
                    } catch (Exception e) {
                        logger.error("Failed to read Parquet metadata, falling back to estimation: {}", e.getMessage());
                        
                        int estimatedRowGroups = estimateRowGroups(fileSize);
                        logger.warn("Using {} estimated row groups", estimatedRowGroups);
                        
                        for (int rowGroupIndex = 0; rowGroupIndex < estimatedRowGroups; rowGroupIndex++) {
                            String rowGroupPartitionKey = String.format("%s_rg_%03d", getPartitionKey(file), rowGroupIndex);
                            
                            writeRowGroupPartition(blockWriter, file, rowGroupPartitionKey, 
                                                 rowGroupIndex, estimatedRowGroups, fileSize, presignedUrl);
                        }
                        
                    }
                    
                } else {
                    String partitionKey = getPartitionKey(file);
                    PartitionInfo partitionInfo = partitionGroups.computeIfAbsent(partitionKey, k -> new PartitionInfo());
                    partitionInfo.addFile(file);
                    
                    if (partitionInfo.getPartitionValues() == null && file.has("partitionValues")) {
                        partitionInfo.setPartitionValues(file.get("partitionValues"));
                    }
                }
            }
            
            for (Map.Entry<String, PartitionInfo> entry : partitionGroups.entrySet()) {
                String partitionKey = entry.getKey();
                PartitionInfo partitionInfo = entry.getValue();
                
                blockWriter.writeRows((Block block, int row) -> {
                    try {
                        if (partitionInfo.getPartitionValues() != null) {
                            JsonNode partitionValues = partitionInfo.getPartitionValues();
                            for (Iterator<String> fieldNames = partitionValues.fieldNames(); fieldNames.hasNext();) {
                                String fieldName = fieldNames.next();
                                JsonNode value = partitionValues.get(fieldName);
                                setBlockValue(block, fieldName, row, value);
                            }
                        }
                        
                        block.setValue(PARTITION_ID_METADATA, row, partitionKey);
                        block.setValue(FILE_COUNT_METADATA, row, partitionInfo.getFileCount());
                        block.setValue(PROCESSING_MODE_METADATA, row, "STANDARD");
                        block.setValue(FILE_SIZE_METADATA, row, 0L);
                        block.setValue(ROW_GROUP_INDEX_METADATA, row, -1);
                        block.setValue(TOTAL_ROW_GROUPS_METADATA, row, 0);
                        
                        return 1;
                    } catch (Exception e) {
                        logger.error("Failed to write partition {}: {}", partitionKey, e.getMessage(), e);
                        return 0;
                    }
                });
            }
            
            
        } catch (Exception e) {
            logger.error("Failed to get partitions for {}.{}.{}: {}", configuredShareName, schemaName, table, e.getMessage(), e);
            throw new RuntimeException("Failed to get table partitions from Delta Share: " + e.getMessage(), e);
        }
    }
    
    /**
     * Write a row group partition with ORDERING PRIORITY
     * Ensures data is processed in correct row group sequence (1, 2, 3, 4, 5)
     */
    private void writeRowGroupPartition(BlockWriter blockWriter, JsonNode file, String partitionKey,
                                       int rowGroupIndex, int totalRowGroups, long fileSize, String presignedUrl)
    {
        blockWriter.writeRows((Block block, int row) -> {
            try {
                if (file.has("partitionValues")) {
                    JsonNode partitionValues = file.get("partitionValues");
                    for (Iterator<String> fieldNames = partitionValues.fieldNames(); fieldNames.hasNext();) {
                        String fieldName = fieldNames.next();
                        JsonNode value = partitionValues.get(fieldName);
                        setBlockValue(block, fieldName, row, value);
                    }
                }
                
                block.setValue(PARTITION_ID_METADATA, row, partitionKey);
                block.setValue(FILE_COUNT_METADATA, row, 1);
                block.setValue(PROCESSING_MODE_METADATA, row, "ROW_GROUP");
                block.setValue(FILE_SIZE_METADATA, row, fileSize);
                block.setValue(PRESIGNED_URL_METADATA, row, presignedUrl);
                block.setValue(ROW_GROUP_INDEX_METADATA, row, rowGroupIndex);
                block.setValue(TOTAL_ROW_GROUPS_METADATA, row, totalRowGroups);
                
                block.setValue("split_order_priority", row, rowGroupIndex);
                
                return 1;
            } catch (Exception e) {
                logger.error("Failed to write row group partition {}: {}", partitionKey, e.getMessage(), e);
                return 0;
            }
        });
        
    }
    
    
    /**
     * Ultra-small partitions for guaranteed Lambda completion
     * Uses 150MB per partition to ensure all row groups complete successfully
     */
    private int estimateRowGroups(long fileSize)
    {
        long targetRowGroupSize = 150L * 1024 * 1024;
        int estimatedGroups = Math.max(1, (int) Math.ceil((double) fileSize / targetRowGroupSize));
        
        return Math.min(estimatedGroups, 20);
    }
    
    /**
     * Enhanced doGetSplits with row group metadata support
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        Map<String, String> partitionMetadata = partitions.getSchema().getCustomMetadata();
        
        try {
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                
                Map<String, String> splitMetadata = new HashMap<>(partitionMetadata);
                
                readPartitionFieldToMetadata(partitions, curPartition, PARTITION_ID_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, FILE_COUNT_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, PROCESSING_MODE_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, PRESIGNED_URL_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, ROW_GROUP_INDEX_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, TOTAL_ROW_GROUPS_METADATA, splitMetadata);
                readPartitionFieldToMetadata(partitions, curPartition, FILE_SIZE_METADATA, splitMetadata);
                
                for (Field field : partitions.getSchema().getFields()) {
                    String fieldName = field.getName();
                    if (!isMetadataField(fieldName)) {
                        FieldReader fieldReader = partitions.getFieldReader(fieldName);
                        if (fieldReader != null) {
                            fieldReader.setPosition(curPartition);
                            Object value = fieldReader.readObject();
                            if (value != null) {
                                splitMetadata.put(fieldName, value.toString());
                            }
                        }
                    }
                }
                
                splitMetadata.put("table_name", request.getTableName().getTableName());
                splitMetadata.put("schema_name", request.getTableName().getSchemaName());
                splitMetadata.put("catalog_name", request.getCatalogName());
                
                SpillLocation spillLocation = makeSpillLocation(request);
                Split split = new Split(spillLocation, makeEncryptionKey(), splitMetadata);
                splits.add(split);
                
                if (splits.size() >= MAX_SPLITS_PER_REQUEST && curPartition != partitions.getRowCount() - 1) {
                    logger.info("Reached max splits per request ({}), returning with continuation token", MAX_SPLITS_PER_REQUEST);
                    return new GetSplitsResponse(request.getCatalogName(), splits, encodeContinuationToken(curPartition + 1));
                }
            }
            
            if (splits.isEmpty()) {
                logger.info("No partitions found, creating fallback split");
                Map<String, String> fallbackMetadata = new HashMap<>(partitionMetadata);
                fallbackMetadata.put(PARTITION_ID_METADATA, "default");
                fallbackMetadata.put(FILE_COUNT_METADATA, "0");
                fallbackMetadata.put(PROCESSING_MODE_METADATA, "STANDARD");
                fallbackMetadata.put("table_name", request.getTableName().getTableName());
                fallbackMetadata.put("schema_name", request.getTableName().getSchemaName());
                fallbackMetadata.put("catalog_name", request.getCatalogName());
                
                SpillLocation spillLocation = makeSpillLocation(request);
                Split split = new Split(spillLocation, makeEncryptionKey(), fallbackMetadata);
                splits.add(split);
            }
            
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
            
        } catch (Exception e) {
            logger.error("Failed to create splits: {}", e.getMessage(), e);
            
            Map<String, String> errorMetadata = new HashMap<>();
            errorMetadata.put(PARTITION_ID_METADATA, "error");
            errorMetadata.put(FILE_COUNT_METADATA, "0");
            errorMetadata.put(PROCESSING_MODE_METADATA, "ERROR");
            errorMetadata.put("table_name", request.getTableName().getTableName());
            errorMetadata.put("schema_name", request.getTableName().getSchemaName());
            errorMetadata.put("error_message", e.getMessage());
            
            SpillLocation spillLocation = makeSpillLocation(request);
            Split split = new Split(spillLocation, makeEncryptionKey(), errorMetadata);
            splits.add(split);
            
            return new GetSplitsResponse(request.getCatalogName(), splits, null);
        }
    }
    
    /**
     * Helper to read partition field value into metadata map
     */
    private void readPartitionFieldToMetadata(Block partitions, int rowIndex, String fieldName, Map<String, String> metadata)
    {
        FieldReader fieldReader = partitions.getFieldReader(fieldName);
        if (fieldReader != null) {
            fieldReader.setPosition(rowIndex);
            Object value = fieldReader.readObject();
            if (value != null) {
                metadata.put(fieldName, value.toString());
            }
        }
    }
    
    /**
     * Check if a field name is a metadata field
     */
    private boolean isMetadataField(String fieldName)
    {
        return PARTITION_ID_METADATA.equals(fieldName) ||
               FILE_COUNT_METADATA.equals(fieldName) ||
               PROCESSING_MODE_METADATA.equals(fieldName) ||
               PRESIGNED_URL_METADATA.equals(fieldName) ||
               ROW_GROUP_INDEX_METADATA.equals(fieldName) ||
               TOTAL_ROW_GROUPS_METADATA.equals(fieldName) ||
               FILE_SIZE_METADATA.equals(fieldName) ||
               JSON_PREDICATE_HINTS_METADATA.equals(fieldName) ||
               PARTITION_VALUES_METADATA.equals(fieldName);
    }
    
    /**
     * Helper methods for pagination support
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            try {
                return Integer.parseInt(request.getContinuationToken());
            } catch (NumberFormatException e) {
                logger.warn("Invalid continuation token: {}", request.getContinuationToken());
                return 0;
            }
        }
        return 0;
    }
    
    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
    
    /**
     * Helper to set block values with proper type handling
     */
    private void setBlockValue(Block block, String fieldName, int row, JsonNode value)
    {
        try {
            Field field = block.getSchema().findField(fieldName);
            if (field == null) {
                logger.warn("Field '{}' not found in block schema, skipping", fieldName);
                return;
            }
            
            if (value == null || value.isNull()) {
                return;
            }
            
            org.apache.arrow.vector.types.pojo.ArrowType fieldType = field.getType();
            
            if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
                logger.warn("UNEXPECTED: Date partition field '{}' reached setBlockValue - converting to string", fieldName);
                String stringValue = value.asText();
                block.setValue(fieldName, row, stringValue);
            } else if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
                try {
                    block.setValue(fieldName, row, value.asLong());
                } catch (Exception e) {
                    logger.warn("Failed to parse integer '{}' for field '{}': {}", value.asText(), fieldName, e.getMessage());
                    block.setValue(fieldName, row, 0L);
                }
            } else if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
                try {
                    block.setValue(fieldName, row, value.asDouble());
                } catch (Exception e) {
                    logger.warn("Failed to parse double '{}' for field '{}': {}", value.asText(), fieldName, e.getMessage());
                    block.setValue(fieldName, row, 0.0);
                }
            } else {
                String stringValue = value.asText();
                block.setValue(fieldName, row, stringValue);
            }
            
        } catch (Exception e) {
            logger.error("Failed to set value for field '{}': {}", fieldName, e.getMessage(), e);
        }
    }
    
    /**
     * Helper class to track partition information
     */
    private static class PartitionInfo
    {
        private JsonNode partitionValues;
        private final List<JsonNode> files = new ArrayList<>();
        
        public JsonNode getPartitionValues()
        {
            return partitionValues;
        }
        
        public void setPartitionValues(JsonNode partitionValues)
        {
            this.partitionValues = partitionValues;
        }
        
        public void addFile(JsonNode file)
        {
            this.files.add(file);
        }
        
        public int getFileCount()
        {
            return files.size();
        }
        
        public List<JsonNode> getFiles()
        {
            return files;
        }
    }
}
