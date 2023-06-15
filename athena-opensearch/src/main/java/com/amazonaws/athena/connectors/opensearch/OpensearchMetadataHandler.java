/*-
 * #%L
 * athena-opensearch
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
package com.amazonaws.athena.connectors.opensearch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.opensearch.OpensearchConstants.OPENSEARCH_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.opensearch.OpensearchConstants.OPENSEARCH_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.opensearch.OpensearchConstants.OPENSEARCH_NAME;

import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.NULLIF_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.NEGATE_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;


/**
 * Handles metadata for Opensearch. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class OpensearchMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of();
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String ALL_PARTITIONS = "*";
    static final String PARTITION_COLUMN_NAME = "partition_name";
    private static final Logger LOGGER = LoggerFactory.getLogger(OpensearchMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private final String schemaName;
    private static final String DATA_STREAM_REGEX = "(\\.ds-)(.+)(-\\d\\d\\d\\d\\d\\d)";

    private OpensearchGlueHandler glueHandler;
    private final boolean awsGlue;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link OpensearchMuxCompositeHandler} instead.
     */
    public OpensearchMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(OPENSEARCH_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public OpensearchMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(OPENSEARCH_DRIVER_CLASS, OPENSEARCH_DEFAULT_PORT)), configOptions);
    }

    public OpensearchMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.schemaName = configOptions.getOrDefault("schema_name", "default");
        
        this.glueHandler = new OpensearchGlueHandler(configOptions);
        this.awsGlue = glueHandler.isDisabled();
    }

    @VisibleForTesting
    protected OpensearchMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
        this.schemaName = configOptions.getOrDefault("schema_name", "default");

        this.glueHandler = new OpensearchGlueHandler(configOptions);
        this.awsGlue = glueHandler.isDisabled();
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        Set<StandardFunctions> unsupportedFunctions = ImmutableSet.of(NULLIF_FUNCTION_NAME, IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, 
        NEGATE_FUNCTION_NAME, LIKE_PATTERN_FUNCTION_NAME, IS_NULL_FUNCTION_NAME, IN_PREDICATE_FUNCTION_NAME);

        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
            FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.EQUATABLE_VALUE_SET
        ));

        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
            TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
            LimitPushdownSubType.INTEGER_CONSTANT
        ));

        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .filter(values -> !unsupportedFunctions.contains(values))
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        Set<String> schemas = new HashSet<>();
        schemas.add(schemaName);
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        blockWriter.writeRows((Block block, int rowNum) -> {
            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
            LOGGER.info("Adding partition {}", ALL_PARTITIONS);
            //we wrote 1 row so we return 1
            return 1;
        });
    }

    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();
        LOGGER.info("Curr Partition: {}, Total Partitions: {}", partitionContd, partitions.getRowCount());

        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);

            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

            LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition + 1));
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        LOGGER.debug("doGetTable: enter - " + getTableRequest);

        Schema schema = null;
        Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());

        // Look at GLUE catalog first.
        try {
            if (!this.awsGlue) {
                schema = glueHandler.doGetTable(blockAllocator, getTableRequest).getSchema();
                LOGGER.info("glueDoGetTable: Retrieved schema for table[{}] from AWS Glue.", getTableRequest.getTableName());
            }
        }
        catch (Exception error) {
            LOGGER.warn("glueDoGetTable: Unable to retrieve table[{}:{}] from AWS Glue.",
            getTableRequest.getTableName().getSchemaName(),
            getTableRequest.getTableName().getTableName(),
                    error);
        }

        if (schema == null) {
            try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
                schema = getSchema(connection, getTableRequest.getTableName(), partitionSchema);
            }
        }

        return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(),
                (schema == null) ? SchemaBuilder.newBuilder().build() : schema, partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
    }

    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        LOGGER.info("Getting Schema for table: {} ", tableName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            boolean found = false;
            while (resultSet.next()) {
                int dataType = resultSet.getInt("DATA_TYPE");
                int columnSize = resultSet.getInt("COLUMN_SIZE");
                int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        dataType,
                        columnSize,
                        decimalDigits,
                        configOptions);
                String columnName = resultSet.getString("COLUMN_NAME");
                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    if (columnType instanceof ArrowType.List) {
                        schemaBuilder.addListField(columnName, getArrayArrowTypeFromTypeName(
                                resultSet.getString("TYPE_NAME"),
                                resultSet.getInt("COLUMN_SIZE"),
                                resultSet.getInt("DECIMAL_DIGITS")));
                    }
                    else if (columnType instanceof ArrowType.Struct) {
                        continue;
                    }
                    else {
                        if (columnName.contains(".")) {
                            FieldBuilder rootField = schemaBuilder.getNestedField(columnName.split("\\.")[0]);
                            FieldBuilder newField = OpensearchUtils.createNestedStruct(columnName, rootField, columnType);
                            schemaBuilder.addNestedField(columnName.split("\\.")[0], newField);
                        }
                        else {
                            schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                        }
                    }
                }
                else {
                    // Default to VARCHAR ArrowType
                    LOGGER.warn("getSchema: Unable to map type for column[" + columnName +
                            "] to a supported type, attempted " + columnType + " - defaulting type to VARCHAR.");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, new ArrowType.Utf8()).build());
                }
                found = true;
            }

            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }

            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);

            return schemaBuilder.build();
        }
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    @Override
    public ListTablesResponse doListTables(final BlockAllocator blockAllocator, final ListTablesRequest listTablesRequest)
            throws Exception
    {
        try (Connection connection = jdbcConnectionFactory.getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List table names for Catalog {}, Table {}", listTablesRequest.getQueryId(), listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
            return new ListTablesResponse(listTablesRequest.getCatalogName(),
                    listTables(connection, listTablesRequest.getSchemaName()), null);
        }
    }

    private List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        try (ResultSet resultSet = getTables(jdbcConnection, databaseName)) {
            ImmutableList.Builder<TableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                // checks if table name is internal table ex: .kibana
                String tableName = resultSet.getString("TABLE_NAME");
                if (tableName.startsWith(".")) {
                    if (!tableName.matches(DATA_STREAM_REGEX)) {
                        continue;
                    }
                }
                // update schema from null because Opensearch doesn't have schema but change later
                // resultSet.updateString("TABLE_SCHEM", databaseName);
                
                // Temporary in schemaName 
                list.add(getSchemaTableName(resultSet, databaseName));
            }
            return list.build();
        }
    }

    private TableName getSchemaTableName(final ResultSet resultSet, String schemaName)
            throws SQLException
    {
        String tableName = resultSet.getString("TABLE_NAME");
        String tableSchema = resultSet.getString("TABLE_SCHEM");
        Pattern pattern = Pattern.compile(DATA_STREAM_REGEX);
        Matcher matcher = pattern.matcher(tableName);
        if (matcher.find()) {
            tableName = matcher.group(2);
        }
        return new TableName(schemaName, tableName);
    }

    private ResultSet getTables(final Connection connection, final String schemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                null,
                new String[] {"TABLE", "VIEW", "EXTERNAL TABLE"});
    }

    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), null), 
                null);
    }
}
