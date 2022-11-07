
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
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.MAX_PARTITION_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.PARTITION_RECORD_COUNT;
import static java.util.Map.entry;

/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns` in
 * information_schema.
 */
public class SnowflakeMetadataHandler extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private static final String COLUMN_NAME = "COLUMN_NAME";
    /**
     * fetching number of records in the table
     */
    static final String COUNT_RECORDS_QUERY = "SELECT row_count\n" +
            "FROM   information_schema.tables\n" +
            "WHERE  table_type = 'BASE TABLE'\n" +
            "AND table_schema= ?\n" +
            "AND TABLE_NAME = ? ";
    private static final String CASE_UPPER = "upper";
    private static final String CASE_LOWER = "lower";
    /**
     * Query to check view
     */
    static final String VIEW_CHECK_QUERY = "SELECT * FROM information_schema.views WHERE table_schema = ? AND table_name = ?";
    static final String ALL_PARTITIONS = "*";
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link SnowflakeMuxCompositeHandler} instead.
     */
    public SnowflakeMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME));
    }

    /**
     * Used by Mux.
     */
    public SnowflakeMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig,
                JDBC_PROPERTIES, new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS,
                SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)));
    }

    @VisibleForTesting
    protected SnowflakeMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
                                       AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    public SnowflakeMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
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
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        /**
         * "PARTITION_RECORD_COUNT" is currently set to 500000.
         * It means there will be 500000 rows per partition. The number of partition will be total number of rows divided by
         * PARTITION_RECORD_COUNT variable value.
         * "MAX_PARTITION_COUNT" is currently set to 50 to limit the number of partitions.
         * this is to handle timeout issues because of huge partitions
         */
        LOGGER.info(" Total Partition Limit" + MAX_PARTITION_COUNT);
        LOGGER.info(" Total Page  Count" +  PARTITION_RECORD_COUNT);
        boolean viewFlag = checkForView(getTableLayoutRequest);
        //if the input table is a view , there will be single split
        if (viewFlag) {
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                return 1;
            });
        }
        else {
            double totalRecordCount = 0;
            LOGGER.info(COUNT_RECORDS_QUERY);
            List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(), getTableLayoutRequest.getTableName().getTableName());

            try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
                 PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection)
                         .withQuery(COUNT_RECORDS_QUERY).withParameters(parameters).build();
                 ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    totalRecordCount = rs.getInt(1);
                }
                if (totalRecordCount > 0) {
                    // if number of partitions are more than defined limit "MAX_PARTITION_COUNT"
                    // it will do maximum 50 partitions,49 partitions will have 500000 records each and last partition will have the remaining number of records.
                    double limitValue = totalRecordCount / PARTITION_RECORD_COUNT;
                    double limit = (int) Math.ceil(limitValue);
                    long offset = 0;
                    if (limit > MAX_PARTITION_COUNT) {
                        for (int i = 1; i <= MAX_PARTITION_COUNT; i++) {
                            int partitionRecord = PARTITION_RECORD_COUNT;
                            if (i == MAX_PARTITION_COUNT) {
                                //Updating partitionRecord variable to display the remaining records in the last partition.
                                //we get the value by subtracting the records displayed till 49th partition from the total number of records.
                                partitionRecord = (int) totalRecordCount - (PARTITION_RECORD_COUNT * (MAX_PARTITION_COUNT - 1));
                            }
                            final String partitionVal = BLOCK_PARTITION_COLUMN_NAME + "-limit-" + partitionRecord + "-offset-" + offset;
                            LOGGER.info("partitionVal {} ", partitionVal);
                            blockWriter.writeRows((Block block, int rowNum) ->
                            {
                                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionVal);
                                return 1;
                            });
                            offset = offset + PARTITION_RECORD_COUNT;
                        }
                    }
                    else {
                        /**
                         * Custom pagination based partition logic will be applied with limit and offset clauses.
                         * the partition values we are setting the limit and offset values like p-limit-3000-offset-0
                         */
                        for (int i = 1; i <= limit; i++) {
                            final String partitionVal = BLOCK_PARTITION_COLUMN_NAME + "-limit-" + PARTITION_RECORD_COUNT + "-offset-" + offset;
                            LOGGER.info("partitionVal {} ", partitionVal);
                            blockWriter.writeRows((Block block, int rowNum) ->
                            {
                                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionVal);
                                return 1;
                            });
                            offset = offset + PARTITION_RECORD_COUNT;
                        }
                    }
                }
                else {
                    LOGGER.info("No Records Found for table {}", getTableLayoutRequest.getTableName().getTableName());
                }
            }
        }
    }

    /**
     * Check if the input table is a view and returns viewflag accordingly
     * @param getTableLayoutRequest
     * @return
     * @throws Exception
     */
    private boolean checkForView(GetTableLayoutRequest getTableLayoutRequest) throws Exception
    {
        boolean viewFlag = false;
        List<String> viewparameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(), getTableLayoutRequest.getTableName().getTableName());
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

    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = getTableFromMetadata(connection.getCatalog(), getTableRequest.getTableName(), connection.getMetaData());
            GetTableResponse getTableResponse = new GetTableResponse(getTableRequest.getCatalogName(), tableName, getSchema(connection, tableName, partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
            return getTableResponse;
        }
    }

    /**
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    public Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        /**
         * query to fetch column data type to handle appropriate datatype to arrowtype conversions.
         */
        String dataTypeQuery = "select COLUMN_NAME, DATA_TYPE from \"INFORMATION_SCHEMA\".\"COLUMNS\" WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            stmt.setString(1, tableName.getSchemaName().toUpperCase());
            stmt.setString(2, tableName.getTableName().toUpperCase());

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
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                String columnName = resultSet.getString(COLUMN_NAME);
                String dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);
                final Map<String, ArrowType> stringArrowTypeMap = Map.ofEntries(
                        entry("INTEGER", Types.MinorType.INT.getType()),
                        entry("DATE", Types.MinorType.DATEDAY.getType()),
                        entry("TIMESTAMP", Types.MinorType.DATEMILLI.getType()),
                        entry("TIMESTAMP_LTZ", Types.MinorType.DATEMILLI.getType()),
                        entry("TIMESTAMP_NTZ", Types.MinorType.DATEMILLI.getType()),
                        entry("TIMESTAMP_TZ", Types.MinorType.DATEMILLI.getType())
                );
                if (dataType != null && stringArrowTypeMap.containsKey(dataType.toUpperCase())) {
                    columnType = stringArrowTypeMap.get(dataType.toUpperCase());
                }
                /**
                 * converting into VARCHAR for not supported data types.
                 */
                if (columnType == null) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }
                if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }

                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    LOGGER.debug(" AddField Schema Building...()  ");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
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
     *
     * @param catalogName
     * @param tableHandle
     * @param metadata
     * @return
     * @throws SQLException
     */
    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    /**
     * Finding table name from query hint
     * In sap hana schemas and tables can be case sensitive, but executed query from athena sends table and schema names
     * in lower case, this has been handled by appending query hint to the table name as below
     * "lambda:lambdaname".SCHEMA_NAME."TABLE_NAME@schemacase=upper&tablecase=upper"
     * @param table
     * @return
     */
    protected  TableName findTableNameFromQueryHint(TableName table)
    {
        //if no query hints has been passed then return input table name
        if (!table.getTableName().contains("@")) {
            return new TableName(table.getSchemaName().toUpperCase(), table.getTableName().toUpperCase());
        }
        //analyze the hint to find table and schema case
        String[] tbNameWithQueryHint = table.getTableName().split("@");
        String[] hintDetails = tbNameWithQueryHint[1].split("&");
        String schemaCase = CASE_UPPER;
        String tableCase = CASE_UPPER;
        String tableName = tbNameWithQueryHint[0];
        for (String str : hintDetails) {
            String[] hintDetail = str.split("=");
            if (hintDetail[0].contains("schema")) {
                schemaCase = hintDetail[1];
            }
            else if (hintDetail[0].contains("table")) {
                tableCase = hintDetail[1];
            }
        }
        if (schemaCase.equalsIgnoreCase(CASE_UPPER) && tableCase.equalsIgnoreCase(CASE_UPPER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_LOWER) && tableCase.equalsIgnoreCase(CASE_LOWER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toLowerCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_LOWER) && tableCase.equalsIgnoreCase(CASE_UPPER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_UPPER) && tableCase.equalsIgnoreCase(CASE_LOWER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toLowerCase());
        }
        else {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
    }

    /**
     * Logic to handle case sensitivity of table name and schema name
     * @param catalogName
     * @param tableHandle
     * @param metadata
     * @return
     * @throws SQLException
     */
    protected TableName getTableFromMetadata(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        TableName tableName = findTableNameFromQueryHint(tableHandle);
        //check for presence exact table and schema name returned by findTableNameFromQueryHint method by invoking metadata.getTables method
        ResultSet resultSet = metadata.getTables(catalogName, tableName.getSchemaName(), tableName.getTableName(), null);
        while (resultSet.next()) {
            if (tableName.getTableName().equals(resultSet.getString(3))) {
                tableName = new TableName(tableName.getSchemaName(), resultSet.getString(3));
                return tableName;
            }
        }
        // if table not found in above step, check for presence of input table by doing pattern search
        ResultSet rs = metadata.getTables(catalogName, tableName.getSchemaName().toUpperCase(), "%", null);
        while (rs.next()) {
            if (tableName.getTableName().equalsIgnoreCase(rs.getString(3))) {
                tableName = new TableName(tableName.getSchemaName().toUpperCase(), rs.getString(3));
                return tableName;
            }
        }
        return tableName;
    }
    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List schema names for Catalog {}", listSchemasRequest.getQueryId(), listSchemasRequest.getCatalogName());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), listDatabaseNames(connection));
        }
    }
    protected static Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws Exception
    {
        try (ResultSet resultSet = jdbcConnection.getMetaData().getSchemas()) {
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
