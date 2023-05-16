/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2020 Amazon web services
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
package com.amazonaws.athena.connectors.cloudera;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ImpalaMetadataHandler extends JdbcMetadataHandler
{
    static final Logger LOGGER = LoggerFactory.getLogger(ImpalaMetadataHandler.class);
    static final String GET_METADATA_QUERY = "describe FORMATTED ";
    public ImpalaMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(ImpalaConstants.IMPALA_NAME, configOptions), configOptions);
    }
    public ImpalaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, new ImpalaJdbcConnectionFactory(databaseConnectionConfig, ImpalaConstants.JDBC_PROPERTIES, new DatabaseConnectionInfo(ImpalaConstants.IMPALA_DRIVER_CLASS, ImpalaConstants.IMPALA_DEFAULT_PORT)), configOptions);
    }

    @VisibleForTesting
    protected ImpalaMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfiguration,
        AWSSecretsManager secretManager,
        AmazonAthena athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfiguration, secretManager, athena, jdbcConnectionFactory, configOptions);
    }

    /**
     * {@inheritDoc}
     */
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(TopNPushdownSubType.SUPPORTS_ORDER_BY));

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Delegates creation of partition schema to database type implementation.
     *
     * @param catalogName Athena provided Impala catalog name.
     * @return schema. See {@link Schema}
     */
    @Override
    public Schema getPartitionSchema(String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder().addField(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME,
                Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     * Used to get the Impala partitions that must be read from the request table in order to satisfy the requested predicate.
     *
     * @param blockWriter Used to write rows (Impala partitions) into the Apache Arrow response.
     * @param getTableLayoutRequest Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws Exception An Exception should be thrown for database connection failures , query syntax errors and so on.
     **/
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             Statement stmt = connection.createStatement();
             PreparedStatement psmt = connection.prepareStatement(GET_METADATA_QUERY + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase())) {
            Map<String, String> columnHashMap = getMetadataForGivenTable(psmt);
            String tableType = columnHashMap.get("TableType");
            if (tableType == null) {
                ResultSet partitionRs = stmt.executeQuery("show files in " + getTableLayoutRequest.getTableName().getQualifiedTableName().toUpperCase());
                Set<String> partition = new HashSet<>();
                while (partitionRs != null && partitionRs.next()) {
                    String partitionString = partitionRs.getString("Partition");
                    if (partitionString != null && !partitionString.isEmpty()) {
                        partition.add(partitionString);
                    }
                }
                LOGGER.debug("isTablePartitioned:" + !partition.isEmpty());
                if (!partition.isEmpty()) {
                    addPartitions(partition, columnHashMap, blockWriter);
                }
                else {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, ImpalaConstants.ALL_PARTITIONS);
                        return 1;
                    });
                }
            }
            else {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, ImpalaConstants.ALL_PARTITIONS);
                    return 1;
                });
            }
        }
    }
    /**
     *  Used to write all Impala partitions into the response.
     * @param partitionInfo Holds all Impala partitions for a table.
     * @param columnInfo  Holds all column names and data types for a table.
     * @param blockWriter Used to write rows (Impala partitions) into the Apache Arrow response.
     */
    private  void addPartitions(Set<String> partitionInfo, Map<String, String> columnInfo, BlockWriter blockWriter)
    {
        Iterator<String> partitions = partitionInfo.iterator();
        while (partitions.hasNext()) {
            String partition = partitions.next();
            String[] partitionColumns = partition.split("/");
            int partitionCounter = 0;
            StringBuilder columnCondition = new StringBuilder();
            while (partitionColumns.length > partitionCounter) {
                String partitionValue = partitionColumns[partitionCounter].split("=")[1];
                String columnName = partitionColumns[partitionCounter].split("=")[0];
                String columnType = columnInfo.get(columnName).toUpperCase();
                if (partitionValue.equalsIgnoreCase("__HIVE_DEFAULT_PARTITION__")) {
                    columnCondition.append(" " + columnName).append(" is").append(" NULL");
                }
                else {
                    if (columnType != null && (columnType.equalsIgnoreCase("STRING") || columnType.equalsIgnoreCase("VARCHAR"))) {
                        columnCondition.append(" " + columnName).append("=").append("'");
                        columnCondition.append(partitionValue).append("'");
                    }
                    else {
                        columnCondition.append(" " + columnName).append("=");
                        columnCondition.append(partitionValue);
                    }
                }
                partitionCounter++;
                if (partitionColumns.length > partitionCounter) {
                    columnCondition.append(" and");
                }
            }
            final String partitionValue = columnCondition.toString();
            LOGGER.debug("partitionValue:" + partitionValue);
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionValue);
                return 1;
            });
        }
    }
    /**
     * Used to split-up the reads required to scan the requested batch of partition(s).
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param getSplitsRequest Provides details of the Impala catalog, database, table, and partition(s) being queried as well as
     * any filter predicate.
     * @return A GetSplitsResponse which primarily contains:
     * 1. A Set of Splits which represent read operations Amazon Athena must perform by calling your read function.
     * 2. (Optional) A continuation token which allows you to paginate the generation of splits for large queries.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(),
                getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);
            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(ImpalaConstants.BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));
            splits.add(splitBuilder.build());
            if (splits.size() >= ImpalaConstants.MAX_SPLITS_PER_REQUEST) {
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits,
                        encodeContinuationToken(curPartition));
            }
        }
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
    /**
     * Used to get definition (field names, types, descriptions, etc...) of an Impala Table.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param getTableRequest Provides details on who made the request and which Athena catalog, database, and Impala table they are querying.
     * @return A GetTableResponse which primarily contains:
     * 1. An Apache Arrow Schema object describing the table's columns, types, and descriptions.
     * 2. A Set of Strings of partition column names (or empty if the table isn't partitioned).
     */
    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(),
                    getSchema(connection, getTableRequest.getTableName(), partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
        }
    }

    /**
     * Used to convert Impala data types to Apache arrow data types
     * @param jdbcConnection  A JDBC Impala database connection
     * @param tableName   Holds table name and schema name. see {@link TableName}
     * @param partitionSchema A partition schema for a given table .See {@link Schema}
     * @return Schema  Holds Table schema along with partition schema. See {@link Schema}
     * @throws Exception An Exception should be thrown for database connection failures , query syntax errors and so on.
     */
    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema) throws Exception
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement psmt = connection.prepareStatement(
                GET_METADATA_QUERY + tableName.getQualifiedTableName().toUpperCase())) {
                Map<String, String> hashMap = getMetadataForGivenTable(psmt);
                while (resultSet.next()) {
                    ArrowType columnType = JdbcArrowTypeConverter.toArrowType(resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"), resultSet.getInt("DECIMAL_DIGITS"), configOptions);
                    String columnName = resultSet.getString(ImpalaConstants.COLUMN_NAME);
                    String dataType = hashMap.get(columnName);
                    LOGGER.debug("columnName:" + columnName);
                    LOGGER.debug("dataType:" + dataType);
                    /**
                     * Converting date data type into DATEDAY MinorType
                     */
                    if (dataType != null && dataType.toUpperCase().contains("DATE")) {
                        columnType = Types.MinorType.DATEDAY.getType();
                    }
                    /**
                     * Converting binary data type into VARBINARY MinorType
                     */

                    if (dataType != null && dataType.toUpperCase().contains("BINARY")) {
                        columnType = Types.MinorType.VARBINARY.getType();
                    }
                    /**
                     * Converting double data type into FLOAT8 MinorType
                     */
                    if (dataType != null && dataType.toUpperCase().contains("DOUBLE")) {
                        columnType = Types.MinorType.FLOAT8.getType();
                    }
                    /**
                     * Converting boolean data type into BIT MinorType
                     */
                    if (dataType != null && dataType.toUpperCase().contains("BOOLEAN")) {
                        columnType = Types.MinorType.BIT.getType();
                    }
                    /**
                     * Converting float data type into FLOAT4 MinorType
                     */
                    if (dataType != null && dataType.toUpperCase().contains("FLOAT")) {
                        columnType = Types.MinorType.FLOAT4.getType();
                    }
                    /**
                     * Converting TIMESTAMP data type into DATEMILLI MinorType
                     */
                    if (dataType != null && dataType.toUpperCase().contains("TIMESTAMP")) {
                        columnType = Types.MinorType.DATEMILLI.getType();
                    }
                    /**
                     * Converting other data type into VARCHAR MinorType
                     */
                    if (columnType == null) {
                        columnType = Types.MinorType.VARCHAR.getType();
                    }
                    if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                        columnType = Types.MinorType.VARCHAR.getType();
                    }

                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                }
            }
            partitionSchema.getFields().forEach(schemaBuilder::addField);
            return schemaBuilder.build();
        }
    }
    /**
     *  used to get all Arrow metadata information about a table.
     * @param catalogName  catalog name
     * @param tableHandle Holds table name and schema name. see {@link TableName}
     * @param metadata  Database metadata
     * @return A result set contains table metadata (data type , size and so on).
     * @throws SQLException A SQLException should be thrown for database connection failures , query syntax errors and so on.
     */
    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(catalogName, escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape), null);
    }

    /**
     *  used to get column names and associated data types for column names.
     * @param statement A PreparedStatement holds query to get metadata for a table.
     * @return Map of column name and associated data type for column.
     * @throws SQLException A SQLException should be thrown for database connection failures , query syntax errors and so on.
     */
    private Map<String, String> getMetadataForGivenTable(PreparedStatement statement) throws SQLException
    {
        Map<String, String> columnHashMap = new HashMap<>();
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                String dataType = rs.getString(ImpalaConstants.METADATA_COLUMN_TYPE);
                if (dataType != null && !dataType.isEmpty() && dataType.toUpperCase().contains("VIEW")) {
                    columnHashMap.put("TableType", "VIEW");
                }
                else
                if (dataType != null && !dataType.isEmpty()) {
                    columnHashMap.put(rs.getString(ImpalaConstants.METADATA_COLUMN_NAME).trim(), dataType.trim());
                }
            }
        }
        return columnHashMap;
    }
}
