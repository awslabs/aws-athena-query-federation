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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlServerMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerMetadataHandler.class);

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String ALL_PARTITIONS = "0";
    static final String PARTITION_NUMBER = "PARTITION_NUMBER";
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
    static final String GET_PARTITIONS_QUERY = "select distinct PARTITION_NUMBER from SYS.DM_DB_PARTITION_STATS where object_id = OBJECT_ID(?) and partition_number > 1 "; //'dbo.MyPartitionTable'
    static final String ROW_COUNT_QUERY = "select count(distinct PARTITION_NUMBER) as row_count from SYS.DM_DB_PARTITION_STATS where object_id = OBJECT_ID(?) and partition_number > 1 ";

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

    public SqlServerMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SqlServerConstants.NAME));
    }
    /**
     * Used by Mux.
     */
    public SqlServerMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new SqlServerJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(SqlServerConstants.DRIVER_CLASS, SqlServerConstants.DEFAULT_PORT)));
    }
    public SqlServerMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }
    @VisibleForTesting
    protected SqlServerMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
                                       AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }
    @Override
    public Schema getPartitionSchema(String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(PARTITION_NUMBER, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
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
        String viewFlag = "N";
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(VIEW_CHECK_QUERY).withParameters(params).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                viewFlag = "VIEW".equalsIgnoreCase(resultSet.getString("TYPE_DESC")) ? "Y" : "N";
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
                if ("Y".equals(viewFlag) || rowCount == 0) {
                    LOGGER.debug("Getting as single Partition: ");
                    blockWriter.writeRows((Block block, int rowNum) ->
                    {
                        block.setValue(PARTITION_NUMBER, rowNum, ALL_PARTITIONS);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
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
        }
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

    /**
     * @param blockAllocator
     * @param getTableRequest
     * @return
     */
    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = new TableName(getTableRequest.getTableName().getSchemaName().toUpperCase(), getTableRequest.getTableName().getTableName().toUpperCase());
            return new GetTableResponse(getTableRequest.getCatalogName(), tableName, getSchema(connection, tableName, partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
        }
    }

    /**
     * Appropriate datatype to arrow type conversions will be done by fetching data types of columns
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        String dataTypeQuery = "SELECT C.NAME AS COLUMN_NAME, TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE " +
                "FROM SYS.COLUMNS C " +
                "JOIN SYS.TYPES T " +
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
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                columnName = resultSet.getString("COLUMN_NAME");

                dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);

                /**
                 * Converting date data type into DATEDAY since framework is unable to do it by default
                 */
                if ("date".equalsIgnoreCase(dataType)) {
                    columnType = Types.MinorType.DATEDAY.getType();
                }
                /**
                 * Converting bit data type into TINYINT because BIT type is showing 0 as false and 1 as true.
                 * we can avoid it by changing to TINYINT.
                 */
                if ("bit".equalsIgnoreCase(dataType)) {
                    columnType = Types.MinorType.TINYINT.getType();
                }
                /**
                 * Converting tinyint data type into SMALLINT.
                 * TINYINT range is 0 to 255 in SQL Server, usage of TINYINT(ArrowType) leads to data loss
                 * as its using 1 bit as signed flag.
                 */
                if ("tinyint".equalsIgnoreCase(dataType)) {
                    columnType = Types.MinorType.SMALLINT.getType();
                }
                /**
                 * Converting numeric, smallmoney data types into FLOAT8 to avoid data loss
                 * (ex: 123.45 is shown as 123 (loosing its scale))
                 */
                if ("numeric".equalsIgnoreCase(dataType) || "smallmoney".equalsIgnoreCase(dataType)) {
                    columnType = Types.MinorType.FLOAT8.getType();
                }
                /**
                 * Converting time data type(s) into DATEMILLI since framework is unable to map it by default
                 */
                if ("datetime".equalsIgnoreCase(dataType) || "datetime2".equalsIgnoreCase(dataType)
                        || "smalldatetime".equalsIgnoreCase(dataType) || "datetimeoffset".equalsIgnoreCase(dataType)) {
                    columnType = Types.MinorType.DATEMILLI.getType();
                }
                /**
                 * converting into VARCHAR for non supported data types.
                 */
                if (columnType == null) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }
                if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }

                LOGGER.debug("columnType: " + columnType);
                if (columnType != null && SupportedTypes.isSupported(columnType)) {
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
            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);
            return schemaBuilder.build();
        }
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

    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List schema names for Catalog {}", listSchemasRequest.getQueryId(), listSchemasRequest.getCatalogName());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), listDatabaseNames(connection));
        }
    }

    private Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        String queryToListUserCreatedSchemas = "select s.name as schema_name from " +
                "sys.schemas s inner join sys.sysusers u on u.uid = s.principal_id " +
                "where u.issqluser = 1 " +
                "and u.name not in ('sys', 'guest', 'INFORMATION_SCHEMA') " +
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
}
