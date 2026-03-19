/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.CredentialsProviderFactory;
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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;
import com.amazonaws.athena.connectors.sqlserver.resolver.SQLServerJDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;

public class SqlServerMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerMetadataHandler.class);

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String ALL_PARTITIONS = "0";
    static final String PARTITION_FUNCTION = "PARTITION_FUNCTION";
    static final String PARTITIONING_COLUMN = "PARTITIONING_COLUMN";

    /**
     * A table can have indexes, partitions. Based on index_id, partition_number we can distinguish whether the table is partitioned or not
     * table does not have index and partition - no rows will be returned
     * table does not have index but has partition - rows will be returned with different partition_number
     * table have indexes but does not have partition - no rows will be returned
     * (When non-partitioned table have indexes partition_number will be 1, so these rows will be skipped with where condition)
     * table have both index and partition - rows will be returned with different partition_number as we are using distinct in the query.
     */
    static final String GET_PARTITIONS_QUERY = "select distinct PARTITION_NUMBER from sys.dm_db_partition_stats where object_id = OBJECT_ID(?) and partition_number > 1 "; //'dbo.MyPartitionTable'
    static final String ROW_COUNT_QUERY = "select count(distinct PARTITION_NUMBER) as row_count from sys.dm_db_partition_stats where object_id = OBJECT_ID(?) and partition_number > 1 ";

    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    /**
     * Query for retrieving Sql Server table partition details
     */
    static final String GET_PARTITION_DETAILS_QUERY = "SELECT " +
            "c.name AS [Partitioning Column], " +
            "       pf.name AS [Partition Function] " +
            "FROM sys.tables AS t   " +
            "JOIN sys.indexes AS i   " +
            "    ON t.[object_id] = i.[object_id]   " +
            "    AND i.[type] <= 1 " +
            "JOIN sys.partition_schemes AS ps   " +
            "    ON ps.data_space_id = i.data_space_id   " +
            "JOIN sys.partition_functions pf ON pf.function_id = ps.function_id " +
            "JOIN sys.index_columns AS ic   " +
            "    ON ic.[object_id] = i.[object_id]   " +
            "    AND ic.index_id = i.index_id   " +
            "    AND ic.partition_ordinal >= 1 " +
            "JOIN sys.columns AS c   " +
            "    ON t.[object_id] = c.[object_id]   " +
            "    AND ic.column_id = c.column_id   " +
            "WHERE t.object_id = (select object_id from sys.objects o where o.name = ? " +
            "and schema_id = (select schema_id from sys.schemas s where s.name = ?))";
    static final String VIEW_CHECK_QUERY = "select TYPE_DESC from sys.objects where name = ? and schema_id = (select schema_id from sys.schemas s where s.name = ?)";
    static final String LIST_PAGINATED_TABLES_QUERY = "SELECT o.name AS \"TABLE_NAME\", s.name AS \"TABLE_SCHEM\" FROM sys.objects o INNER JOIN sys.schemas s ON o.schema_id = s.schema_id WHERE o.type IN ('U', 'V') and s.name = ? ORDER BY TABLE_NAME OFFSET ? ROWS FETCH NEXT ? ROWS ONLY;";

    public SqlServerMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SqlServerConstants.NAME, configOptions), configOptions);
    }
    /**
     * Used by Mux.
     */
    public SqlServerMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(SqlServerConstants.DRIVER_CLASS, SqlServerConstants.DEFAULT_PORT)), configOptions);
    }

    public SqlServerMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, new SQLServerJDBCCaseResolver(SqlServerConstants.NAME));
    }

    @VisibleForTesting
    protected SqlServerMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions,
        JDBCCaseResolver caseResolver)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions, caseResolver);
    }

    @Override
    public Schema getPartitionSchema(String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(PARTITION_NUMBER, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    @VisibleForTesting
    protected List<TableName> getPaginatedTables(Connection connection, String databaseName, int token, int limit) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setInt(2, token);
        preparedStatement.setInt(3, limit);
        LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    @Override
    protected ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        String token = listTablesRequest.getNextToken();
        int pageSize = listTablesRequest.getPageSize();

        int t = token != null ? Integer.parseInt(token) : 0;

        LOGGER.info("Starting pagination at {} with page size {}", token, pageSize);
        List<TableName> paginatedTables = getPaginatedTables(connection, listTablesRequest.getSchemaName(), t, pageSize);
        LOGGER.info("{} tables returned. Next token is {}", paginatedTables.size(), t + pageSize);
        // return next token is null when reaching end of files
        return new ListTablesResponse(listTablesRequest.getCatalogName(),
                paginatedTables,
                paginatedTables.isEmpty() || paginatedTables.size() < pageSize ? null : Integer.toString(t + pageSize)
        );
    }
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();

        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Check whether input table is a view or not. If it's a view, it will not have any partition info and
     * data will be fetched with single split.If it's a table with no partition, then data will be fetched with single split.
     * If it's a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
     * for parallel processing.
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     * @throws Exception
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        List<String> params = Arrays.asList(getTableLayoutRequest.getTableName().getTableName(), getTableLayoutRequest.getTableName().getSchemaName());

        //check whether the input table is a view or not
        boolean viewFlag = false;
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(VIEW_CHECK_QUERY).withParameters(params).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                viewFlag = "VIEW".equalsIgnoreCase(resultSet.getString("TYPE_DESC"));
            }
            LOGGER.info("viewFlag: {}", viewFlag);
        }

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName() + "." +
                    getTableLayoutRequest.getTableName().getTableName());
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
                 PreparedStatement preparedStatement2 = new PreparedStatementBuilder().withConnection(connection).withQuery(ROW_COUNT_QUERY).withParameters(parameters).build();
                 ResultSet resultSet = preparedStatement.executeQuery();
                 ResultSet resultSet2 = preparedStatement2.executeQuery()) {
                int rowCount = 0;
                // check whether the table have partitions or not using ROW_COUNT_QUERY
                if (resultSet2.next()) {
                    rowCount = resultSet2.getInt("ROW_COUNT");
                    LOGGER.info("rowCount: {}", rowCount);
                }

                // create a single split for view/non-partition table
                if (viewFlag || rowCount == 0) {
                    handleSinglePartition(blockWriter);
                }
                else {
                    LOGGER.debug("Getting data with diff Partitions: ");
                    // get partition details from sql server meta data tables
                    List<String> partitionDetails = getPartitionDetails(params);
                    String partitionInfo = (!partitionDetails.isEmpty() && partitionDetails.size() == 2) ?
                            ":::" + partitionDetails.get(0) + ":::" + partitionDetails.get(1) : "";

                    // Include the first partition because it's not retrieved from GET_PARTITIONS_QUERY
                    blockWriter.writeRows((Block block, int rowNum) ->
                    {
                        block.setValue(PARTITION_NUMBER, rowNum, "1" + partitionInfo);
                        return 1;
                    });
                    if (resultSet.next()) {
                        do {
                            final String partitionNumber = resultSet.getString(PARTITION_NUMBER);
                            // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                            // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                            blockWriter.writeRows((Block block, int rowNum) ->
                            {
                                block.setValue(PARTITION_NUMBER, rowNum, partitionNumber + partitionInfo);
                                //we wrote 1 row so we return 1
                                return 1;
                            });
                        }
                        while (resultSet.next());
                    }
                }
            }
            catch (SQLServerException e) {
                // For permission denied SQL Server exception, return single partition
                // SQL Server 2022 and later: "VIEW DATABASE PERFORMANCE STATE permission denied"
                String message = e.getMessage();
                if (message != null && (message.contains("VIEW DATABASE STATE permission denied")
                        || message.contains("VIEW DATABASE PERFORMANCE STATE permission denied"))) {
                    LOGGER.warn("Permission denied while accessing partition metadata: {}", message);
                    handleSinglePartition(blockWriter);
                }
                else {
                    throw e;
                }
            }
        }
    }

    private static void handleSinglePartition(BlockWriter blockWriter)
    {
        LOGGER.debug("Getting as single Partition: ");
        blockWriter.writeRows((Block block, int rowNum) -> {
            block.setValue(PARTITION_NUMBER, rowNum, ALL_PARTITIONS);
            return 1;
        });
    }

    /**
     * @param blockAllocator
     * @param getSplitsRequest
     * @return
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }

        int partitionContd = decodeContinuationToken(getSplitsRequest);
        LOGGER.info("partitionContd: {}", partitionContd);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(PARTITION_NUMBER);
            locationReader.setPosition(curPartition);
            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
            LOGGER.debug("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());
            Split.Builder splitBuilder;
            String partInfo = String.valueOf(locationReader.readText());

            // Included partition information to split if the table is partitioned
            if (partInfo.contains(":::")) {
                String[] partInfoAr = partInfo.split(":::");
                splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(PARTITION_NUMBER, partInfoAr[0])
                        .add(PARTITION_FUNCTION, partInfoAr[1])
                        .add(PARTITIONING_COLUMN, partInfoAr[2]);
            }
            else {
                splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(PARTITION_NUMBER, partInfo);
            }
            splits.add(splitBuilder.build());
            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition));
            }
        }
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    /**
     * If the table have partitions fetch those partition details from sql server metadata tables.
     * This information will be used while forming the custom query to get specific partition as a split
     * @param parameters
     * @throws SQLException
     */
    private List<String> getPartitionDetails(List<String> parameters) throws Exception
    {
        List<String> partitionDetails = new ArrayList<>();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITION_DETAILS_QUERY).withParameters(parameters).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                partitionDetails.add(resultSet.getString("PARTITION FUNCTION"));
                partitionDetails.add(resultSet.getString("PARTITIONING COLUMN"));
                LOGGER.debug("partitionFunction: {}", partitionDetails.get(0));
                LOGGER.debug("partitioningColumn: {}", partitionDetails.get(1));
            }
        }
        return partitionDetails;
    }

    @Override
    protected Optional<ArrowType> convertDatasourceTypeToArrow(int columnIndex, int precision, Map<String, String> configOptions, ResultSetMetaData metadata) throws SQLException
    {
        String dataType = metadata.getColumnTypeName(columnIndex);
        LOGGER.info("In convertDatasourceTypeToArrow: converting {}", dataType);
        if (dataType != null && SqlServerDataType.isSupported(dataType)) {
            LOGGER.debug("Sql Server  Datatype is support: {}", dataType);
            return Optional.of(SqlServerDataType.fromType(dataType));
        }
        return super.convertDatasourceTypeToArrow(columnIndex, precision, configOptions, metadata);
    }

    /**
     * Appropriate datatype to arrow type conversions will be done by fetching data types of columns
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema, AwsRequestOverrideConfiguration requestOverrideConfiguration)
            throws Exception
    {
        String dataTypeQuery = "SELECT C.NAME AS COLUMN_NAME, TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE " +
                "FROM sys.columns C " +
                "JOIN sys.types T " +
                "ON C.USER_TYPE_ID=T.USER_TYPE_ID " +
                "WHERE C.OBJECT_ID=OBJECT_ID(?)";

        String dataType;
        String columnName;
        HashMap<String, String> hashMap = new HashMap<>();
        boolean found = false;

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            // fetch data types of columns and prepare map with column name and datatype.
            stmt.setString(1, tableName.getSchemaName() + "." + tableName.getTableName());
            try (ResultSet dataTypeResultSet = stmt.executeQuery()) {
                while (dataTypeResultSet.next()) {
                    dataType = dataTypeResultSet.getString("DATA_TYPE");
                    columnName = dataTypeResultSet.getString("COLUMN_NAME");
                    hashMap.put(columnName.trim(), dataType.trim());
                }
            }

            while (resultSet.next()) {
                Optional<ArrowType> columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);
                columnName = resultSet.getString("COLUMN_NAME");

                dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);

                if (dataType != null && SqlServerDataType.isSupported(dataType)) {
                    columnType = Optional.of(SqlServerDataType.fromType(dataType));
                }
                /**
                 * converting into VARCHAR for non supported data types.
                 */
                if (columnType.isEmpty() || !SupportedTypes.isSupported(columnType.get())) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                LOGGER.debug("columnType: " + columnType);
                if (columnType.isPresent() && SupportedTypes.isSupported(columnType.get())) {
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            }
            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }
            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);
            return schemaBuilder.build();
        }
    }

    @Override
    protected Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        String queryToListUserCreatedSchemas = "select s.name as schema_name from " +
                "sys.schemas s " +
                "where s.name not in ('sys', 'guest', 'INFORMATION_SCHEMA', 'db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin', 'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin') " +
                "order by s.name";
        try (Statement st = jdbcConnection.createStatement();
                ResultSet resultSet = st.executeQuery(queryToListUserCreatedSchemas)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name");
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
    }

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        return CredentialsProviderFactory.createCredentialProvider(
                getDatabaseConnectionConfig().getSecret(),
                getCachableSecretsManager(),
                new SqlServerOAuthCredentialsProvider()
        );
    }
}
