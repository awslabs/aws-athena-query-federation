
/*-
 * #%L
 * athena-saphana
 * %%
 * Copyright (C) 2019  Amazon Web Services
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
package com.amazonaws.athena.connectors.saphana;

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
import com.amazonaws.athena.connectors.saphana.resolver.SaphanaJDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.DATA_TYPE_QUERY_FOR_TABLE;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.DATA_TYPE_QUERY_FOR_VIEW;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_NAME;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.TO_WELL_KNOWN_TEXT_FUNCTION;

public class SaphanaMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaphanaMetadataHandler.class);

    public static final String LIST_PAGINATED_TABLES_QUERY =
            "SELECT OBJECT_NAME AS \"TABLE_NAME\", SCHEMA_NAME AS \"TABLE_SCHEM\" " +
                    "FROM SYS.OBJECTS " +
                    "WHERE SCHEMA_NAME = ? AND OBJECT_TYPE IN ('TABLE', 'VIEW') " +
                    "ORDER BY OBJECT_NAME " +
                    "LIMIT ? OFFSET ?";

    public SaphanaMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SaphanaConstants.SAPHANA_NAME, configOptions), configOptions);
    }
    /**
     * Used by Mux.
     */
    public SaphanaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig,
                SaphanaConstants.JDBC_PROPERTIES, new DatabaseConnectionInfo(SaphanaConstants.SAPHANA_DRIVER_CLASS,
                SaphanaConstants.SAPHANA_DEFAULT_PORT)), configOptions);
    }
    @VisibleForTesting
    protected SaphanaMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions,
        JDBCCaseResolver caseResolver)
    {
        super(databaseConnectionConfig,
                secretsManager,
                athena,
                jdbcConnectionFactory,
                configOptions,
                caseResolver);
    }

    public SaphanaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
            super(databaseConnectionConfig,
                    jdbcConnectionFactory,
                    configOptions,
                    new SaphanaJDBCCaseResolver(SAPHANA_NAME));
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

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     * We are first checking if input table is a view, if it's a view, it will not have any partition info and
     * data will be fetched with single split.If it is a table with no partition, then data will be fetched with single split.
     * If it is a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
     * for parallel processing.
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     * @throws Exception
     *
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.debug("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());

        //check if the input table is a view
        boolean viewFlag = false;
        List<String> viewparameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(), getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(SaphanaConstants.VIEW_CHECK_QUERY).withParameters(viewparameters).build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    viewFlag = true;
                }
                LOGGER.debug("viewFlag: {}", viewFlag);
            }
        }
        //For view create a single split
        if (viewFlag) {
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, SaphanaConstants.ALL_PARTITIONS);
                return 1;
            });
        }
        else {
            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getTableName(),
                    getTableLayoutRequest.getTableName().getSchemaName());
            try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
                try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection)
                        .withQuery(SaphanaConstants.GET_PARTITIONS_QUERY).withParameters(parameters).build();
                     ResultSet resultSet = preparedStatement.executeQuery()) {
                    // Return a single partition if no partitions defined
                    if (!resultSet.next()) {
                        blockWriter.writeRows((Block block, int rowNum) ->
                        {
                            block.setValue(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, SaphanaConstants.ALL_PARTITIONS);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    else {
                        do {
                            final String partitionName = resultSet.getString(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME);
                            // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                            // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                            blockWriter.writeRows((Block block, int rowNum) ->
                            {
                                block.setValue(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                                //we wrote 1 row so we return 1
                                return 1;
                            });
                        }
                        while (resultSet.next());
                    }
                }
            }
        }
    }

    @Override
    public ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        LOGGER.debug("Starting listPaginatedTables for Saphana.");
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

    /**
     *
     * @param blockAllocator
     * @param getSplitsRequest
     * @return
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.debug("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }
        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Set<Split> splits = new HashSet<>();
        Block partitions = getSplitsRequest.getPartitions();
        // TODO consider splitting further depending on #rows or data size. Could use Hash key for splitting if no partitions.
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME);
            locationReader.setPosition(curPartition);
            SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
            LOGGER.debug("{}: Input partition is {}", getSplitsRequest.getQueryId(), locationReader.readText());
            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(SaphanaConstants.BLOCK_PARTITION_COLUMN_NAME, String.valueOf(locationReader.readText()));
            splits.add(splitBuilder.build());
            if (splits.size() >= SaphanaConstants.MAX_SPLITS_PER_REQUEST) {
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
        LOGGER.debug("SaphanaMetadataHandler:getSchema starting");
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            HashMap<String, String> hashMap = new HashMap<String, String>();
            // fetch data types for columns for appropriate datatype to arrowtype conversions.
            ResultSet dataTypeResultSet = getColumnDatatype(connection, tableName);
            String type = "";
            String name = "";

            while (dataTypeResultSet.next()) {
                type = dataTypeResultSet.getString("DATA_TYPE");
                name = dataTypeResultSet.getString(SaphanaConstants.COLUMN_NAME);
                hashMap.put(name.trim().toLowerCase(), type.trim());
            }

            LOGGER.debug("Data types resolved by column names {}", hashMap);

            if (hashMap.isEmpty() == true) {
                LOGGER.debug("No data type  available for TABLE in hashmap : " + tableName.getTableName());
            }

            boolean found = false;
            while (resultSet.next()) {
                boolean isSpatialDataType = false;
                String columnName = resultSet.getString(SaphanaConstants.COLUMN_NAME);

                LOGGER.debug("SaphanaMetadataHandler:getSchema determining column type of column {}", columnName);
                Optional<ArrowType> columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);

                LOGGER.debug("SaphanaMetadataHandler:getSchema column type of column {} is {}",
                        columnName, columnType);
                String dataType = hashMap.get(columnName.toLowerCase());
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);
                /**
                 * Converting ST_POINT/ST_GEOMETRY data type into VARCHAR
                 */
                if (dataType != null
                        && (dataType.contains("ST_POINT") || dataType.contains("ST_GEOMETRY"))) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                    isSpatialDataType = true;
                }
                /*
                 * converting into VARCHAR for Unsupported data types.
                 */
                if (columnType.isEmpty() || !SupportedTypes.isSupported(columnType.get())) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                if (columnType.isPresent() && SupportedTypes.isSupported(columnType.get())) {
                    LOGGER.debug("Adding column {} to schema of type {}", columnName, columnType.get());

                    if (isSpatialDataType) {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get())
                                .addField(new Field(quoteColumnName(columnName) + TO_WELL_KNOWN_TEXT_FUNCTION,
                                        new FieldType(true, columnType.get(), null), com.google.common.collect.ImmutableList.of()))
                                .build());
                    }
                    else {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
                    }

                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            }

            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }

            partitionSchema.getFields().forEach(schemaBuilder::addField);
        }
        LOGGER.debug(schemaBuilder.toString());
        return schemaBuilder.build();
    }

    /**
     * Logic to fetch column datatypes of table and view to handle data-type specific logic
     * @param connection
     * @param tableName
     * @return
     * @throws SQLException
     */
    private ResultSet getColumnDatatype(Connection connection, TableName tableName)
            throws SQLException
    {
        List<String> statementParams = Arrays.asList(tableName.getTableName(),
                tableName.getSchemaName());
        LOGGER.debug("SaphanaMetadataHandler::getColumnDatatype statement params  {}", statementParams);
        PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection)
                .withQuery(DATA_TYPE_QUERY_FOR_TABLE).withParameters(statementParams).build();
        ResultSet dataTypeResultSet = preparedStatement.executeQuery();
        if (!dataTypeResultSet.isBeforeFirst()) {
            PreparedStatement preparedStatementForView = new PreparedStatementBuilder()
                    .withConnection(connection).withQuery(DATA_TYPE_QUERY_FOR_VIEW).withParameters(statementParams).build();
            ResultSet dataTypeResultSetView = preparedStatementForView.executeQuery();
            return dataTypeResultSetView;
        }
        else {
            LOGGER.debug("Metadata available for TABLE: " + tableName.getTableName());
            return dataTypeResultSet;
        }
    }

    private String quoteColumnName(String columnName)
    {
        columnName = columnName.replace(SAPHANA_QUOTE_CHARACTER, SAPHANA_QUOTE_CHARACTER + SAPHANA_QUOTE_CHARACTER);
        return SAPHANA_QUOTE_CHARACTER + columnName + SAPHANA_QUOTE_CHARACTER;
    }
}
