/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
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
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Db2MetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2MetadataHandler.class);
    static final String PARTITION_NUMBER = "PARTITION_NUMBER";
    static final String PARTITIONING_COLUMN = "PARTITIONING_COLUMN";
    /**
     * DB2 has max number of partition 32,000
     */
    private static final int MAX_SPLITS_PER_REQUEST = 32000;

    /**
     * Instantiates handler to be used by Lambda function directly.
     * <p>
     * Recommend using {@link Db2MuxCompositeHandler} instead.
     */
    public Db2MetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(Db2Constants.NAME));
    }

    /**
     * TO be used by Mux.
     *
     * @param databaseConnectionConfig
     */
    public Db2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, null, new DatabaseConnectionInfo(Db2Constants.DRIVER_CLASS, Db2Constants.DEFAULT_PORT)));
    }

    /**
     * To be used by Mux.
     *
     * @param databaseConnectionConfig
     * @param jdbcConnectionFactory
     */
    public Db2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }

    @VisibleForTesting
    protected Db2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
                                 AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    /**
     * Overridden this method to fetch only user defined schema(s) in Athena Data window.
     *
     * @param blockAllocator
     * @param listSchemasRequest
     * @return
     */
    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List schema names for Catalog {}", listSchemasRequest.getQueryId(), listSchemasRequest.getCatalogName());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), getSchemaList(connection, Db2Constants.QRY_TO_LIST_SCHEMAS));
        }
    }

    /**
     * Overridden this method to fetch table(s) for selected schema in Athena Data window.
     *
     * @param blockAllocator
     * @param listTablesRequest
     * @return
     */
    @Override
    public ListTablesResponse doListTables(final BlockAllocator blockAllocator, final ListTablesRequest listTablesRequest) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List table names for Catalog {}, Schema {}", listTablesRequest.getQueryId(), listTablesRequest.getCatalogName(), listTablesRequest.getSchemaName());
            List<String> tableNames = getTableList(connection, Db2Constants.QRY_TO_LIST_TABLES_AND_VIEWS, listTablesRequest.getSchemaName());
            List<TableName> tables = tableNames.stream().map(tableName -> new TableName(listTablesRequest.getSchemaName(), tableName)).collect(Collectors.toList());
            return new ListTablesResponse(listTablesRequest.getCatalogName(), tables, null);
        }
    }

    /**
     * Creating TableName object, Schema object for partition framing fields,
     * and Schema object for table fields.
     *
     * @param blockAllocator
     * @param getTableRequest
     * @return
     */
    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = getTableRequest.getTableName();
            Schema schema = getSchema(connection, tableName, partitionSchema);
            Set<String> partitionCols = partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
            return new GetTableResponse(getTableRequest.getCatalogName(), tableName, schema, partitionCols);
        }
    }

    /**
     * Creates Schema object with arrow compatible filed to frame the partition.
     *
     * @param catalogName Athena provided catalog name.
     * @return
     */
    @Override
    public Schema getPartitionSchema(String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(PARTITION_NUMBER, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     * A partition is represented by a partition column(s) of type varchar. In case of Db2 connector, partitions are created using below organization schemes.
     * - Distribute by Hash
     * - Partition by range
     * - Organize by dimensions
     * The partition details such as no. of partitions, column name are fetched from the Db2 metadata table(s). A custom query is then used to get the partition.
     * Based upon the number of distinct partitions received, the splits are being created.
     *
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

        List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(), getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(Db2Constants.PARTITION_QUERY).withParameters(parameters).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            // check whether the table have partitions or not using PARTITION_QUERY, create a single split for view/non-partition table
            if (!resultSet.next()) {
                LOGGER.debug("Getting as single Partition: ");
                blockWriter.writeRows((Block block, int rowNum) ->
                {
                    block.setValue(PARTITION_NUMBER, rowNum, "0");
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
            else {
                LOGGER.debug("Getting data with diff Partitions: ");
                // get partition details from DB2 meta data tables
                String columnName = getColumnName(parameters);

                do {
                    // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                    // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                    blockWriter.writeRows((Block block, int rowNum) ->
                    {
                        block.setValue(PARTITION_NUMBER, rowNum, columnName + ":::" + resultSet.getString("DATAPARTITIONID"));
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                } while (resultSet.next());
            }
        }
    }

    /**
     * Split(s) will be created based on table partition. We are taking ContinuationToken from
     * GetSplitsRequest object and loop upto row count of the partitions, and inside loop we are
     * creating Split objects. In each Split object we are adding properties to be used in partition framing.
     *
     * @param blockAllocator
     * @param getSplitsRequest
     * @return
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());

        int partitionContd = decodeContinuationToken(getSplitsRequest);
        Block partitions = getSplitsRequest.getPartitions();
        SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
        Split.Builder splitBuilder;
        Set<Split> splits = new HashSet<>();

        LOGGER.debug("partitionContd: {}", partitionContd);

        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader locationReader = partitions.getFieldReader(PARTITION_NUMBER);
            locationReader.setPosition(curPartition);
            String partInfo = String.valueOf(locationReader.readText());

            LOGGER.debug("{}: Input partition is {}", getSplitsRequest.getQueryId(), partInfo);

            // We are passing partition column and partition number for each split(),
            // so that it can be used while framing the query where clause in
            // QueryStringBuilder as partition identification.
            // Included partition information to split if the table is partitioned
            if (partInfo.contains(":::")) {
                String[] partInfoAr = partInfo.split(":::");
                splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(PARTITIONING_COLUMN, partInfoAr[0])
                        .add(PARTITION_NUMBER, partInfoAr[1]);
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

    /**
     * If the table have partitions fetch those partition details from DB2 metadata tables.
     * This information will be used while forming the custom query to get specific partition as a split.
     *
     * @param parameters
     * @throws Exception
     */
    private String getColumnName(List<String> parameters) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(Db2Constants.COLUMN_INFO_QUERY).withParameters(parameters).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getString("COLNAME");
            }
        }
        return null;
    }

    /**
     * Gets continuationToken from GetSplitsRequest.
     * if not found or not numeric it returns 0.
     *
     * @param request
     * @return
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        return 0;
    }

    /**
     * Converts int value to string value.
     *
     * @param partition
     * @return
     */
    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    /**
     * Logic to fetch schema name(s) from Db2 database.
     * Through jdbc call and executing sql query pulling all the schema names from Db2.
     *
     * @param connection
     * @param query
     * @return List<String>
     * @throws Exception
     */
    private List<String> getSchemaList(final Connection connection, String query) throws Exception
    {
        List<String> list = new ArrayList<>();
        try (Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(query)) {
            while (rs.next()) {
                list.add(rs.getString("NAME"));
            }
        }
        return list;
    }

    /**
     * Logic to fetch table name(s) for given schema. Through jdbc call and executing sql query pulling
     * all the table names from Db2 for a given schema.
     *
     * @param connection
     * @param query
     * @param schemaName
     * @return List<String>
     * @throws Exception
     */
    private List<String> getTableList(final Connection connection, String query, String schemaName) throws Exception
    {
        List<String> list = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery();) {
                while (rs.next()) {
                    list.add(rs.getString("NAME"));
                }
            }
        }
        return list;
    }

    /**
     * Appropriate data type to arrow type conversions will be done by fetching data types of columns.
     * This function creates the arrow Schema pojo from jdbc database metadata of the table.
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
        String typeName;
        String columnName;
        HashMap<String, String> columnNameMap = new HashMap<>();
        boolean found = false;

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(Db2Constants.COLUMN_INFO_QUERY)) {
            stmt.setString(1, tableName.getSchemaName());
            stmt.setString(2, tableName.getTableName());
            try (ResultSet dataTypeResultSet = stmt.executeQuery()) {
                // fetch data types of columns and prepare map with column name and typeName.
                while (dataTypeResultSet.next()) {
                    columnName = dataTypeResultSet.getString("colname");
                    typeName = dataTypeResultSet.getString("typename");
                    columnNameMap.put(columnName, typeName);
                }

                while (resultSet.next()) {
                    ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS")
                    );
                    columnName = resultSet.getString("COLUMN_NAME");
                    typeName = columnNameMap.get(columnName);

                    /*
                    If arrow type is struct then convert to VARCHAR, because struct is
                    considered as Unhandled type by JdbcRecordHandler's makeExtractor method.
                     */
                    if (columnType != null && columnType.getTypeID().name().equalsIgnoreCase("Struct")) {
                        columnType = Types.MinorType.VARCHAR.getType();
                    }

                    /*
                     * Converting REAL, DOUBLE, DECFLOAT data types into FLOAT8 since framework is unable to map it by default
                     */
                    if ("real".equalsIgnoreCase(typeName) || "double".equalsIgnoreCase(typeName) || "decfloat".equalsIgnoreCase(typeName)) {
                        columnType = Types.MinorType.FLOAT8.getType();
                    }

                    /*
                     * converting into VARCHAR for non supported data types.
                     */
                    if ((columnType == null) || !SupportedTypes.isSupported(columnType)) {
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

                partitionSchema.getFields().forEach(schemaBuilder::addField);
                return schemaBuilder.build();
            }
        }
    }

    /**
     * Gets columns meta information from jdbc DatabaseMetaData object.
     *
     * @param catalogName
     * @param tableHandle
     * @param metadata
     * @return
     * @throws Exception
     */
    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws Exception
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }
}
