/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.HintsSubtype;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;
import com.amazonaws.athena.connectors.postgresql.resolver.PostGreSqlJDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;

/**
 * Handles metadata for PostGreSql. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class PostGreSqlMetadataHandler
        extends JdbcMetadataHandler
{
    // These are public so that redshift can use them from a different package.
    public static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    public static final String GET_PARTITIONS_QUERY = "SELECT nmsp_child.nspname AS child_schema, child.relname AS child FROM pg_inherits JOIN pg_class parent " +
            "ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid = child.oid JOIN pg_namespace nmsp_parent " +
            "ON nmsp_parent.oid = parent.relnamespace JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace where nmsp_parent.nspname = ? " +
            "AND parent.relname = ?";
    public static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    public static final String BLOCK_PARTITION_SCHEMA_COLUMN_NAME = "partition_schema_name";
    private static final String MATERIALIZED_VIEWS = "Materialized Views";
    public static final String ALL_PARTITIONS = "*";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlMetadataHandler.class);
    private static final String PARTITION_SCHEMA_NAME = "child_schema";
    private static final String PARTITION_NAME = "child";
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    static final String LIST_PAGINATED_TABLES_QUERY = "SELECT a.\"TABLE_NAME\", a.\"TABLE_SCHEM\" FROM ((SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?) UNION (SELECT matviewname as \"TABLE_NAME\", schemaname as \"TABLE_SCHEM\" from pg_catalog.pg_matviews mv where has_table_privilege(format('%I.%I', mv.schemaname, mv.matviewname), 'select') and schemaname = ?)) AS a ORDER BY a.\"TABLE_NAME\" LIMIT ? OFFSET ?";

    //Session Property Flag that hints to the engine that the data source is using none default collation
    protected static final String NON_DEFAULT_COLLATE = "non_default_collate";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
     */
    public PostGreSqlMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions), configOptions);
    }

    public PostGreSqlMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig,
                new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT)),
                configOptions,
                new PostGreSqlJDBCCaseResolver(POSTGRES_NAME));
    }

    @VisibleForTesting
    protected PostGreSqlMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions, new PostGreSqlJDBCCaseResolver(POSTGRES_NAME));
    }

    @VisibleForTesting
    protected PostGreSqlMetadataHandler(
            DatabaseConnectionConfig databaseConnectionConfig,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            JdbcConnectionFactory jdbcConnectionFactory,
            java.util.Map<String, String> configOptions,
            JDBCCaseResolver resolver)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions, resolver);
    }

    // This is used by Redshift connector to extends
    protected PostGreSqlMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory genericJdbcConnectionFactory, java.util.Map<String, String> configOptions, JDBCCaseResolver resolver)
    {
        super(databaseConnectionConfig, genericJdbcConnectionFactory, configOptions, resolver);
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

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        //Provide a hint to the engine that postgresql is using default collate settings
        //Which doesn't match Athena's Engine Collation; this disabling Predicate pushdown
        boolean nonDefaultCollate = Boolean.valueOf(this.configOptions.getOrDefault(NON_DEFAULT_COLLATE, "false"));
        if (nonDefaultCollate) {
            capabilities.put(DataSourceOptimizations.DATA_SOURCE_HINTS.withSupportedSubTypes(HintsSubtype.NON_DEFAULT_COLLATE));
        }

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Catalog {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(getRequestOverrideConfig(getTableLayoutRequest)))) {
            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(),
                    getTableLayoutRequest.getTableName().getTableName());
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
                    ResultSet resultSet = preparedStatement.executeQuery()) {
                // Return a single partition if no partitions defined
                if (!resultSet.next()) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                else {
                    do {
                        final String partitionSchemaName = resultSet.getString(PARTITION_SCHEMA_NAME);
                        final String partitionName = resultSet.getString(PARTITION_NAME);

                        // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                        // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                        blockWriter.writeRows((Block block, int rowNum) -> {
                            block.setValue(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, rowNum, partitionSchemaName);
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    while (resultSet.next());
                }
            }
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("doGetSplits, QueryId:{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }

        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();

        boolean splitterUsed = false;
        if (partitions.getRowCount() == 1) {
            FieldReader partitionsSchemaFieldReader = partitions.getFieldReader(BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
            partitionsSchemaFieldReader.setPosition(0);
            FieldReader partitionsFieldReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
            partitionsFieldReader.setPosition(0);

            if (ALL_PARTITIONS.equals(partitionsSchemaFieldReader.readText().toString()) && ALL_PARTITIONS.equals(partitionsFieldReader.readText().toString())) {
                for (String splitClause : getSplitClauses(getSplitsRequest.getTableName())) {
                    //Every split must have a unique location if we wish to spill to avoid failures
                    SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

                    Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                            .add(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, String.valueOf(partitionsSchemaFieldReader.readText()))
                            .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(splitClause));

                    splits.add(splitBuilder.build());

                    if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                        throw new RuntimeException("Max splits supported with splitter " + MAX_SPLITS_PER_REQUEST);
                    }

                    splitterUsed = true;
                }
            }
        }

        if (!splitterUsed) {
            for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
                FieldReader partitionsSchemaFieldReader = partitions.getFieldReader(BLOCK_PARTITION_SCHEMA_COLUMN_NAME);
                partitionsSchemaFieldReader.setPosition(curPartition);
                FieldReader partitionsFieldReader = partitions.getFieldReader(BLOCK_PARTITION_COLUMN_NAME);
                partitionsFieldReader.setPosition(curPartition);

                //Every split must have a unique location if we wish to spill to avoid failures
                SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);

                LOGGER.info("{}: Input partition is {}", getSplitsRequest.getQueryId(), String.valueOf(partitionsFieldReader.readText()));
                Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(BLOCK_PARTITION_SCHEMA_COLUMN_NAME, String.valueOf(partitionsSchemaFieldReader.readText()))
                        .add(BLOCK_PARTITION_COLUMN_NAME, String.valueOf(partitionsFieldReader.readText()));

                splits.add(splitBuilder.build());

                if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                    //We exceeded the number of split we want to return in a single request, return and provide a continuation token.
                    return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, encodeContinuationToken(curPartition + 1));
                }
            }
        }

        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    @Override
    protected ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        String token = listTablesRequest.getNextToken();
        int pageSize = listTablesRequest.getPageSize();

        int t = token != null ? Integer.parseInt(token) : 0;
        LOGGER.info("Starting pagination at {} with page size {}", token, pageSize);
        List<TableName> paginatedTables = getPaginatedResults(connection, listTablesRequest.getSchemaName(), t, pageSize);
        LOGGER.info("{} tables returned. Next token is {}", paginatedTables.size(), t + pageSize);
        String nextToken = paginatedTables.isEmpty() || paginatedTables.size() < pageSize ? null : Integer.toString(t + pageSize);

        return new ListTablesResponse(listTablesRequest.getCatalogName(), paginatedTables, nextToken);
    }

    protected List<TableName> getPaginatedResults(Connection connection, String databaseName, int token, int limit) throws SQLException
    {
        PreparedStatement preparedStatement = connection.prepareStatement(LIST_PAGINATED_TABLES_QUERY);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setString(2, databaseName);
        preparedStatement.setInt(3, limit);
        preparedStatement.setInt(4, token);
        LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
        return JDBCUtil.getTableMetadata(preparedStatement, TABLES_AND_VIEWS);
    }

    /*
    Override because we need to return tables and MV views
     */
    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        ImmutableList.Builder<TableName> list = ImmutableList.builder();

        // Gets list of Tables and Views using Information Schema.tables
        list.addAll(JDBCUtil.getTables(jdbcConnection, databaseName));
        // Gets list of Materialized Views using table pg_catalog.pg_matviews 
        list.addAll(getMaterializedViews(jdbcConnection, databaseName));

        return list.build();
    }

    // Add no op method in redshift because no materialized view, they get treated as regular views
    private List<TableName> getMaterializedViews(Connection connection, String databaseName) throws SQLException
    {
        String sql = "select matviewname as \"TABLE_NAME\", schemaname as \"TABLE_SCHEM\" from pg_catalog.pg_matviews mv where has_table_privilege(format('%I.%I', mv.schemaname, mv.matviewname), 'select') and schemaname = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, databaseName);
        LOGGER.debug("Prepared Statement for getting Materialized View in schema {} : {}", databaseName, preparedStatement);
        return JDBCUtil.getTableMetadata(preparedStatement, MATERIALIZED_VIEWS);
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

    /**
     * Converts an ARRAY column's TYPE_NAME (provided by the jdbc metadata) to an ArrowType.
     * @param typeName The column's TYPE_NAME (e.g. _int4, _text, _float8, etc...)
     * @param precision Used for BigDecimal ArrowType
     * @param scale Used for BigDecimal ArrowType
     * @return ArrowType equivalent of the fieldType.
     */
    @Override
    protected ArrowType getArrayArrowTypeFromTypeName(String typeName, int precision, int scale)
    {
        switch (typeName) {
            case "_bool":
                return Types.MinorType.BIT.getType();
            case "_int2":
                return Types.MinorType.SMALLINT.getType();
            case "_int4":
                return Types.MinorType.INT.getType();
            case "_int8":
                return Types.MinorType.BIGINT.getType();
            case "_float4":
                return Types.MinorType.FLOAT4.getType();
            case "_float8":
                return Types.MinorType.FLOAT8.getType();
            case "_date":
                return Types.MinorType.DATEDAY.getType();
            case "_timestamp":
                return Types.MinorType.DATEMILLI.getType();
            case "_numeric":
                return new ArrowType.Decimal(precision, scale);
            default:
                return super.getArrayArrowTypeFromTypeName(typeName, precision, scale);
        }
    }

    /**
     * Retrieves the names of columns with the data type 'CHAR' for a specified table in a PostgreSQL/Redshift database.
     *
     * @param connection the JDBC connection to the database
     * @param schema Postgresql/Redshift schema name
     * @param table Postgresql/Redshift table name
     * @return a list of column names that have the data type 'CHAR'
     * @throws SQLException if a database access error occurs
     */
    public static List<String> getCharColumns(Connection connection, String schema, String table) throws SQLException
    {
        List<String> charColumns = new ArrayList<>();
        String query = "SELECT column_name " +
                "FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? AND data_type = 'character'";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    charColumns.add(resultSet.getString("column_name"));
                }
            }
        }
        return charColumns;
    }

    protected String wrapNameWithEscapedCharacter(String input)
    {
        return "\"" + input + "\"";
    }
}
