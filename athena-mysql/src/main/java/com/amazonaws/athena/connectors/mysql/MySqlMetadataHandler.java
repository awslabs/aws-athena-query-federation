/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler.TABLES_AND_VIEWS;
import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_NAME;

/**
 * Handles metadata for MySQL. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class MySqlMetadataHandler
        extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String GET_PARTITIONS_QUERY = "SELECT DISTINCT partition_name FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? " +
            "AND partition_name IS NOT NULL";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition_name";
    static final String ALL_PARTITIONS = "*";
    static final String PARTITION_COLUMN_NAME = "partition_name";
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    static final String LIST_PAGINATED_TABLES_QUERY = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ? ORDER BY TABLE_NAME LIMIT ?, ?";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link MySqlMuxCompositeHandler} instead.
     */
    public MySqlMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(MYSQL_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public MySqlMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(MYSQL_DRIVER_CLASS, MYSQL_DEFAULT_PORT)), configOptions);
    }

    public MySqlMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
    }

    @VisibleForTesting
    protected MySqlMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
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

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
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
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            final String escape = connection.getMetaData().getSearchStringEscape();

            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getTableName(), getTableLayoutRequest.getTableName().getSchemaName());
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(GET_PARTITIONS_QUERY).withParameters(parameters).build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                // Return a single partition if no partitions defined
                if (!resultSet.next()) {
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                        LOGGER.info("Adding partition {}", ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                else {
                    do {
                        final String partitionName = resultSet.getString(PARTITION_COLUMN_NAME);

                        // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                        // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                        blockWriter.writeRows((Block block, int rowNum) -> {
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                            LOGGER.info("Adding partition {}", partitionName);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    while (resultSet.next() && queryStatusChecker.isQueryRunning());
                }
            }
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
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

        return new ListTablesResponse(listTablesRequest.getCatalogName(), paginatedTables, Integer.toString(t + pageSize));
    }

    @Override
    protected List<TableName> listTables(final Connection jdbcConnection, final String databaseName)
            throws SQLException
    {
        // Gets list of Tables and Views using Information Schema.tables

        return JDBCUtil.getTables(jdbcConnection, databaseName);
    }

    @Override
    protected TableName caseInsensitiveTableSearch(Connection connection, final String databaseName,
                                                     final String tableName) throws Exception
    {
        return JDBCUtil.informationSchemaCaseInsensitiveTableMatch(connection, databaseName, tableName);
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
}
