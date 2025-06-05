
/*-
 * #%L
 * athena-snowflake
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

package com.amazonaws.athena.connectors.snowflake;

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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.util.PaginationHelper;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;
import com.amazonaws.athena.connectors.snowflake.resolver.SnowflakeJDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.MAX_PARTITION_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SINGLE_SPLIT_LIMIT_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_NAME;

/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns` in
 * information_schema.
 */
public class SnowflakeMetadataHandler extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA", "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE", "true");
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private static final String COLUMN_NAME = "COLUMN_NAME";
    static final String LIST_PAGINATED_TABLES_QUERY =
            "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = ? " +
                    "ORDER BY TABLE_NAME " +
                    "LIMIT ? OFFSET ?";
    /**
     * fetching number of records in the table
     */
    static final String COUNT_RECORDS_QUERY = "SELECT row_count\n" +
            "FROM   information_schema.tables\n" +
            "WHERE  table_type = 'BASE TABLE'\n" +
            "AND table_schema= ?\n" +
            "AND TABLE_NAME = ? ";
    static final String SHOW_PRIMARY_KEYS_QUERY = "SHOW PRIMARY KEYS IN ";
    static final String PRIMARY_KEY_COLUMN_NAME = "column_name";
    static final String COUNTS_COLUMN_NAME = "COUNTS";
    /**
     * Query to check view
     */
    static final String VIEW_CHECK_QUERY = "SELECT * FROM information_schema.views WHERE table_schema = ? AND table_name = ?";
    static final String ALL_PARTITIONS = "*";

    static final Map<String, ArrowType> STRING_ARROW_TYPE_MAP = com.google.common.collect.ImmutableMap.of(
            "INTEGER", (ArrowType) Types.MinorType.INT.getType(),
            "DATE", (ArrowType) Types.MinorType.DATEDAY.getType(),
            "TIMESTAMP", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_LTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_NTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_TZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPLTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPNTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPTZ", (ArrowType) Types.MinorType.DATEMILLI.getType()
    );
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link SnowflakeMuxCompositeHandler} instead.
     */
    public SnowflakeMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                new GenericJdbcConnectionFactory(databaseConnectionConfig, SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS, SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)),
                configOptions);
    }

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, new SnowflakeJDBCCaseResolver(SNOWFLAKE_NAME));
    }

    @VisibleForTesting
    protected SnowflakeMetadataHandler(
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
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        LOGGER.debug("doGetDataSourceCapabilities: " + request);
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
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        LOGGER.debug("getPartitionSchema: " + catalogName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    private Optional<String> getPrimaryKey(TableName tableName) throws Exception
    {
        LOGGER.debug("getPrimaryKey tableName: " + tableName);
        List<String> primaryKeys = new ArrayList<String>();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(SHOW_PRIMARY_KEYS_QUERY + "\"" + tableName.getSchemaName() + "\".\"" + tableName.getTableName() + "\"");
                ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    // Concatenate multiple primary keys if they exist
                    primaryKeys.add(rs.getString(PRIMARY_KEY_COLUMN_NAME));
                }
            }

            String primaryKeyString = primaryKeys.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
            if (!Strings.isNullOrEmpty(primaryKeyString) && hasUniquePrimaryKey(tableName, primaryKeyString)) {
                return Optional.of(primaryKeyString);
            }
        }
        return Optional.empty(); 
    }

    /**
    * Snowflake does not enforce primary key constraints, so we double-check user has unique primary key
    * before partitioning.
    */
    private boolean hasUniquePrimaryKey(TableName tableName, String primaryKey) throws Exception 
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + primaryKey +  ", count(*) as COUNTS FROM " + "\"" + tableName.getSchemaName() + "\".\"" + tableName.getTableName() + "\"" + " GROUP BY " + primaryKey + " ORDER BY COUNTS DESC");
                 ResultSet rs = preparedStatement.executeQuery()) {
                if (rs.next()) {
                    if (rs.getInt(COUNTS_COLUMN_NAME) == 1) {
                        // Since it is in descending order and 1 is this first count seen, 
                        // this table has a unique primary key 
                        return true;
                    }   
                }
            }
        }
        LOGGER.warn("Primary key ,{}, is not unique. Falling back to single partition...", primaryKey);
        return false;
    }

    /**
     * Snowflake manual partition logic based upon number of records
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     * @throws Exception
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.debug("getPartitions: {}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            TableName tableName = getTableLayoutRequest.getTableName();
            /**
             * "MAX_PARTITION_COUNT" is currently set to 50 to limit the number of partitions.
             * this is to handle timeout issues because of huge partitions
             */
            LOGGER.info(" Total Partition Limit" + MAX_PARTITION_COUNT);
            boolean viewFlag = checkForView(tableName);
            //if the input table is a view , there will be single split
            if (viewFlag) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    return 1;
                });
                return;
            }

            double totalRecordCount = 0;
            LOGGER.info(COUNT_RECORDS_QUERY);

            try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                    .withConnection(connection)
                    .withQuery(COUNT_RECORDS_QUERY)
                    .withParameters(Arrays.asList(tableName.getSchemaName(), tableName.getTableName())).build();
                    ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    totalRecordCount = rs.getLong(1);
                }
                if (totalRecordCount > 0) {
                    Optional<String> primaryKey = getPrimaryKey(tableName);
                    long recordsInPartition = (long) (Math.ceil(totalRecordCount / MAX_PARTITION_COUNT));
                    long partitionRecordCount = (totalRecordCount <= SINGLE_SPLIT_LIMIT_COUNT || !primaryKey.isPresent()) ? (long) totalRecordCount : recordsInPartition;
                    LOGGER.info(" Total Page Count: " + partitionRecordCount);
                    double numberOfPartitions = (int) Math.ceil(totalRecordCount / partitionRecordCount);
                    long offset = 0;
                    /**
                     * Custom pagination based partition logic will be applied with limit and offset clauses.
                     * It will have maximum 50 partitions and number of records in each partition is decided by dividing total number of records by 50
                     * the partition values we are setting the limit and offset values like p-limit-3000-offset-0
                     */
                    for (int i = 1; i <= numberOfPartitions; i++) {
                        final String partitionVal = BLOCK_PARTITION_COLUMN_NAME + "-primary-" + primaryKey.orElse("") + "-limit-" + partitionRecordCount + "-offset-" + offset;
                        LOGGER.info("partitionVal {} ", partitionVal);
                        blockWriter.writeRows((Block block, int rowNum) ->
                        {
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionVal);
                            return 1;
                        });
                        offset = offset + partitionRecordCount;
                    }
                }
                else {
                    LOGGER.info("No Records Found for table {}", tableName);
                }
            }
        }
    }

    @Override
    public ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        LOGGER.debug("Starting listPaginatedTables for Snowflake.");
        int pageSize = listTablesRequest.getPageSize();
        int token = PaginationHelper.validateAndParsePaginationArguments(listTablesRequest.getNextToken(), pageSize);

        if (pageSize == UNLIMITED_PAGE_SIZE_VALUE) {
            pageSize = Integer.MAX_VALUE;
        }

        String adjustedSchemaName = caseResolver.getAdjustedSchemaNameString(connection, listTablesRequest.getSchemaName(), configOptions);

        LOGGER.info("Starting pagination at {} with page size {}", token, pageSize);
        List<TableName> paginatedTables = getPaginatedTables(connection, adjustedSchemaName, token, pageSize);
        String nextToken = PaginationHelper.calculateNextToken(token, pageSize, paginatedTables);
        LOGGER.info("{} tables returned. Next token is {}", paginatedTables.size(), nextToken);

        return new ListTablesResponse(listTablesRequest.getCatalogName(), paginatedTables, nextToken);
    }

    @VisibleForTesting
    protected List<TableName> getPaginatedTables(Connection connection, String databaseName, int offset, int limit) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);

        preparedStatement.setString(1, databaseName);
        preparedStatement.setInt(2, limit);
        preparedStatement.setInt(3, offset);

        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    /*
     * Check if the input table is a view and returns viewflag accordingly
     */
    private boolean checkForView(TableName tableName) throws Exception
    {
        boolean viewFlag = false;
        List<String> viewparameters = Arrays.asList(tableName.getSchemaName(), tableName.getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(VIEW_CHECK_QUERY).withParameters(viewparameters).build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    viewFlag = true;
                }
                LOGGER.info("viewFlag: {}", viewFlag);
            }
        }
        return viewFlag;
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("doGetSplits: {}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();
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
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        LOGGER.debug("getSchema start, tableName:" + tableName);
        /**
         * query to fetch column data type to handle appropriate datatype to arrowtype conversions.
         */
        String dataTypeQuery = "select COLUMN_NAME, DATA_TYPE from \"INFORMATION_SCHEMA\".\"COLUMNS\" WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            stmt.setString(1, tableName.getSchemaName());
            stmt.setString(2, tableName.getTableName());

            HashMap<String, String> hashMap = new HashMap<String, String>();
            ResultSet dataTypeResultSet = stmt.executeQuery();

            String type = "";
            String name = "";

            while (dataTypeResultSet.next()) {
                type = dataTypeResultSet.getString("DATA_TYPE");
                name = dataTypeResultSet.getString(COLUMN_NAME);
                hashMap.put(name.trim(), type.trim());
            }
            if (hashMap.isEmpty() == true) {
                LOGGER.debug("No data type  available for TABLE in hashmap : " + tableName.getTableName());
            }
            boolean found = false;
            while (resultSet.next()) {
                Optional<ArrowType> columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);
                String columnName = resultSet.getString(COLUMN_NAME);
                String dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);
                if (dataType != null && STRING_ARROW_TYPE_MAP.containsKey(dataType.toUpperCase())) {
                    columnType = Optional.of(STRING_ARROW_TYPE_MAP.get(dataType.toUpperCase()));
                }
                /**
                 * converting into VARCHAR for not supported data types.
                 */
                if (columnType.isEmpty()) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }
                if (columnType.isPresent() && !SupportedTypes.isSupported(columnType.get())) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                if (columnType.isPresent() && SupportedTypes.isSupported(columnType.get())) {
                    LOGGER.debug(" AddField Schema Building...()  ");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            }
            if (!found) {
                throw new AthenaConnectorException("Could not find table in " + tableName.getSchemaName(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString()).build());
            }
            partitionSchema.getFields().forEach(schemaBuilder::addField);
        }
        catch (SnowflakeSQLException ex) {
            throw new AthenaConnectorException(ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION.toString()).build());
        }
        LOGGER.debug(schemaBuilder.toString());
        return schemaBuilder.build();
    }

    @Override
    protected Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        try (ResultSet resultSet = jdbcConnection
                .getMetaData()
                .getSchemas(jdbcConnection.getCatalog(), null)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            String inputCatalogName = jdbcConnection.getCatalog();
            String inputSchemaName = jdbcConnection.getSchema();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                String catalogName = resultSet.getString("TABLE_CATALOG");
                // skip internal schemas
                boolean shouldAddSchema =
                        ((inputSchemaName == null) || schemaName.equals(inputSchemaName)) &&
                                (!schemaName.equals("information_schema") && catalogName.equals(inputCatalogName));

                if (shouldAddSchema) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
    }
}
