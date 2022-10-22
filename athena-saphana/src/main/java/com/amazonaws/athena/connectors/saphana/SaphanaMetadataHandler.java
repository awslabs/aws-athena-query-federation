
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
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
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
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
import java.util.Set;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.DATA_TYPE_QUERY_FOR_TABLE;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.DATA_TYPE_QUERY_FOR_VIEW;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.SAPHANA_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.saphana.SaphanaConstants.TO_WELL_KNOWN_TEXT_FUNCTION;

public class SaphanaMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaphanaMetadataHandler.class);

    public SaphanaMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SaphanaConstants.SAPHANA_NAME));
    }
    /**
     * Used by Mux.
     */
    public SaphanaMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig,
                SaphanaConstants.JDBC_PROPERTIES, new DatabaseConnectionInfo(SaphanaConstants.SAPHANA_DRIVER_CLASS,
                SaphanaConstants.SAPHANA_DEFAULT_PORT)));
    }
    @VisibleForTesting
    protected SaphanaMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
                                     AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    public SaphanaMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory jdbcConnectionFactory)
    {
            super(databaseConnectionConfig, jdbcConnectionFactory);
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

    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = getTableFromMetadata(getTableRequest.getCatalogName(), getTableRequest.getTableName(), connection.getMetaData());
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
    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
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
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));

                LOGGER.debug("SaphanaMetadataHandler:getSchema column type of column {} is {}",
                        columnName, columnType);
                String dataType = hashMap.get(columnName.toLowerCase());
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);

                InferredColumnType inferredColumnType = InferredColumnType.fromType(dataType);
                columnType = inferredColumnType.columnType;
                isSpatialDataType = inferredColumnType.isSpatialType;

                /**
                 * converting into VARCHAR not supported by Framework.
                 */
                if (columnType == null) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }
                if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }

                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    LOGGER.debug("Adding column {} to schema of type {}", columnName, columnType);

                    if (isSpatialDataType) {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType)
                                .addField(new Field(quoteColumnName(columnName) + TO_WELL_KNOWN_TEXT_FUNCTION,
                                        new FieldType(true, columnType, null), List.of()))
                                .build());
                    }
                    else {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
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

    /**
     * Finding table name from query hint
     * In sap hana schemas and tables can be case sensitive, but executed query from athena sends table and schema names
     * in lower case, this has been handled by appending query hint to the table name as below
     * "lambda:lambdaname".SCHEMA_NAME."TABLE_NAME@schemacase=upper&tablecase=upper"
     * @param table
     * @return
     */
    protected TableName findTableNameFromQueryHint(TableName table)
    {
        //if no query hints has been passed then return input table name
        if (!table.getTableName().contains("@")) {
            return table;
        }
        //analyze the hint to find table and schema case
        String[] tbNameWithQueryHint = table.getTableName().split("@");
        String[] hintDetails = tbNameWithQueryHint[1].split("&");
        String schemaCase = SaphanaConstants.CASE_UPPER;
        String tableCase = SaphanaConstants.CASE_UPPER;
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
        if (schemaCase.equalsIgnoreCase(SaphanaConstants.CASE_UPPER) && tableCase.equalsIgnoreCase(SaphanaConstants.CASE_UPPER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(SaphanaConstants.CASE_LOWER) && tableCase.equalsIgnoreCase(SaphanaConstants.CASE_LOWER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toLowerCase());
        }
        else if (schemaCase.equalsIgnoreCase(SaphanaConstants.CASE_LOWER) && tableCase.equalsIgnoreCase(SaphanaConstants.CASE_UPPER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(SaphanaConstants.CASE_UPPER) && tableCase.equalsIgnoreCase(SaphanaConstants.CASE_LOWER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toLowerCase());
        }
        else {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
    }

    private static class InferredColumnType
    {
        private final ArrowType columnType;
        private final boolean isSpatialType;
        public InferredColumnType(ArrowType columnType, boolean isSpatialType)
        {
            this.columnType = columnType;
            this.isSpatialType = isSpatialType;
        }

        private static InferredColumnType nullType()
        {
            return new InferredColumnType(null, false);
        }

        public static InferredColumnType fromType(String dataType)
        {
            if (dataType != null && (dataType.contains("DECIMAL"))) {
                return new InferredColumnType(Types.MinorType.BIGINT.getType(), false);
            }
            if (dataType != null && (dataType.contains("INTEGER"))) {
                return new InferredColumnType(Types.MinorType.INT.getType(), false);
            }
            if (dataType != null && (dataType.contains("date") || dataType.contains("DATE"))) {
                return new InferredColumnType(Types.MinorType.DATEMILLI.getType(), false);
            }
            /**
             * Converting TIMESTAMP data type into TIMESTAMPMILLI
             */
            if (dataType != null && (dataType.contains("TIMESTAMP"))
            ) {
                return new InferredColumnType(Types.MinorType.DATEMILLI.getType(), false);
            }
            /**
             * Converting ST_POINT data type into VARBINARY
             */
            if (dataType != null
                    && (dataType.contains("ST_POINT") || dataType.contains("ST_GEOMETRY"))
            ) {
                return new InferredColumnType(Types.MinorType.VARCHAR.getType(), true);
            }
            /**
             * Converting DAYDATE data type into DATEDAY
             */
            if (dataType != null
                    && (dataType.contains("DAYDATE") || dataType.contains("DATE"))
            ) {
                return new InferredColumnType(Types.MinorType.DATEDAY.getType(), true);
            }
            return nullType();
        }
    }

    private String quoteColumnName(String columnName)
    {
        columnName = columnName.replace(SAPHANA_QUOTE_CHARACTER, SAPHANA_QUOTE_CHARACTER + SAPHANA_QUOTE_CHARACTER);
        return SAPHANA_QUOTE_CHARACTER + columnName + SAPHANA_QUOTE_CHARACTER;
    }
}
