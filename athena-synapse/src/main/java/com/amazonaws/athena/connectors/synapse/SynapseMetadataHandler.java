/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

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
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupDir;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SynapseMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynapseMetadataHandler.class);

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String ALL_PARTITIONS = "0";
    static final String PARTITION_NUMBER = "PARTITION_NUMBER";
    static final String PARTITION_BOUNDARY_FROM = "PARTITION_BOUNDARY_FROM";
    static final String PARTITION_BOUNDARY_TO = "PARTITION_BOUNDARY_TO";
    static final String PARTITION_COLUMN = "PARTITION_COLUMN";

    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;

    public SynapseMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SynapseConstants.NAME));
    }

    /**
     * Used by Mux.
     */
    public SynapseMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        super(databaseConnectionConfig, new SynapseJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES,
                new DatabaseConnectionInfo(SynapseConstants.DRIVER_CLASS, SynapseConstants.DEFAULT_PORT)));
    }

    @VisibleForTesting
    protected SynapseMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
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
     * Partition metadata queries will be extracted from template files, we can check whether the table is partitioned or not using these queries.
     * If it is a table with no partition, then data will be fetched with single split.
     * If it is a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
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

        /**
         * Queries formed through String Template for retrieving Azure Synapse table partitions
         */
        STGroup stGroup = new STGroupDir("templates", '$', '$');

        ST getPartitionsSt = stGroup.getInstanceOf("getPartitions");
        getPartitionsSt.add("name", getTableLayoutRequest.getTableName().getTableName());
        getPartitionsSt.add("schemaname", getTableLayoutRequest.getTableName().getSchemaName());
        ST rowCountSt = stGroup.getInstanceOf("rowCount");
        rowCountSt.add("name", getTableLayoutRequest.getTableName().getTableName());
        rowCountSt.add("schemaname", getTableLayoutRequest.getTableName().getSchemaName());

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             Statement st = connection.createStatement();
             Statement st2 = connection.createStatement();
             ResultSet resultSet = st.executeQuery(getPartitionsSt.render());
             ResultSet resultSet2 = st2.executeQuery(rowCountSt.render())) {
            int rowCount = 0;
            // check whether the table have partitions or not using ROW_COUNT_QUERY
            if (resultSet2.next()) {
                rowCount = resultSet2.getInt("ROW_COUNT");
                LOGGER.info("rowCount: {}", rowCount);
            }
            // create a single split for view/non-partition table
            if (rowCount == 0) {
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

                // partitionBoundaryTo, partitionColumn can not be declared in loop scope as they need to retain the value for next iteration.
                String partitionBoundaryTo = "0";
                String partitionColumn = "";

                    /*
                    Synapse supports Range Partitioning. Partition column, partition range values are extracted from Synapse metadata tables.
                    partition boundaries will be formed using those values.
                    Ex: if partition column is 'col1', partition range values are 10, 200, null then
                        below partition boundaries will be created to form custom queries for splits
                        1::: :::10:::col1, 2:::10:::200:::col1, 3:::200::: :::col1
                     */
                while (resultSet.next()) {
                    String partitionBoundaryFrom;
                    final String partitionNumber = resultSet.getString(PARTITION_NUMBER);
                    LOGGER.debug("partitionNumber: {}", partitionNumber);
                    if ("1".equals(partitionNumber)) {
                        partitionBoundaryFrom = " ";
                        partitionColumn = resultSet.getString(PARTITION_COLUMN);
                        LOGGER.debug("partitionColumn: {}", partitionColumn);
                    }
                    else {
                        partitionBoundaryFrom = partitionBoundaryTo;
                    }
                    partitionBoundaryTo = resultSet.getString("PARTITION_BOUNDARY_VALUE");
                    partitionBoundaryTo = (partitionBoundaryTo == null) ? " " : partitionBoundaryTo;

                    // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                    // 2. This API is not paginated, we could use order by and limit clause with offsets here.

                    String finalPartitionBoundaryTo = partitionBoundaryTo;
                    String finalPartitionColumn = partitionColumn;
                    blockWriter.writeRows((Block block, int rowNum) ->
                    {
                        // creating the partition boundaries
                        block.setValue(PARTITION_NUMBER, rowNum, partitionNumber + ":::" + partitionBoundaryFrom + ":::" + finalPartitionBoundaryTo + ":::" + finalPartitionColumn);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
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
                        .add(PARTITION_BOUNDARY_FROM, partInfoAr[1])
                        .add(PARTITION_BOUNDARY_TO, partInfoAr[2])
                        .add(PARTITION_COLUMN, partInfoAr[3]);
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
        LOGGER.info("Inside getSchema");

        String dataTypeQuery = "SELECT C.NAME AS COLUMN_NAME, TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE, " +
                "C.PRECISION, C.SCALE " +
                "FROM SYS.COLUMNS C " +
                "JOIN SYS.TYPES T " +
                "ON C.USER_TYPE_ID=T.USER_TYPE_ID " +
                "WHERE C.OBJECT_ID=OBJECT_ID(?)";

        SchemaBuilder schemaBuilder;
        HashMap<String, List<String>> columnNameAndDataTypeMap = new HashMap<>();

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            // fetch data types of columns and prepare map with column name and datatype information.
            stmt.setString(1, tableName.getSchemaName() + "." + tableName.getTableName());
            try (ResultSet dataTypeResultSet = stmt.executeQuery()) {
                while (dataTypeResultSet.next()) {
                    List<String> columnDetails = List.of(
                            dataTypeResultSet.getString("DATA_TYPE").trim(),
                            dataTypeResultSet.getString("PRECISION").trim(),
                            dataTypeResultSet.getString("SCALE").trim());
                    columnNameAndDataTypeMap.put(dataTypeResultSet.getString("COLUMN_NAME").trim(), columnDetails);
                }
            }
        }

        if ("azureServerless".equalsIgnoreCase(SynapseUtil.checkEnvironment(jdbcConnection.getMetaData().getURL()))) {
            // getColumns() method from SQL Server driver is causing an exception in case of Azure Serverless environment.
            // so doing explicit data type conversion
            schemaBuilder = doDataTypeConversion(columnNameAndDataTypeMap, tableName.getSchemaName());
        }
        else {
            schemaBuilder = doDataTypeConversionForNonCompatible(jdbcConnection, tableName, columnNameAndDataTypeMap);
        }
        // add partition columns
        partitionSchema.getFields().forEach(schemaBuilder::addField);
        return schemaBuilder.build();
    }

    private SchemaBuilder doDataTypeConversion(HashMap<String, List<String>> columnNameAndDataTypeMap, String schemaName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        for (Map.Entry<String, List<String>> entry : columnNameAndDataTypeMap.entrySet()) {
            String columnName = entry.getKey();
            List<String> dataTypeDetails = entry.getValue();
            String dataType = dataTypeDetails.get(0);
            ArrowType columnType = Types.MinorType.VARCHAR.getType();

            LOGGER.debug("columnName: " + columnName);
            LOGGER.debug("dataType: " + dataType);

            if ("char".equalsIgnoreCase(dataType) || "varchar".equalsIgnoreCase(dataType) || "binary".equalsIgnoreCase(dataType) ||
                    "nchar".equalsIgnoreCase(dataType) || "nvarchar".equalsIgnoreCase(dataType) || "varbinary".equalsIgnoreCase(dataType)
                    || "time".equalsIgnoreCase(dataType) || "uniqueidentifier".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.VARCHAR.getType();
            }

            if ("bit".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.TINYINT.getType();
            }

            if ("tinyint".equalsIgnoreCase(dataType) || "smallint".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.SMALLINT.getType();
            }

            if ("int".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.INT.getType();
            }

            if ("bigint".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.BIGINT.getType();
            }

            if ("decimal".equalsIgnoreCase(dataType) || "money".equalsIgnoreCase(dataType)) {
                columnType = ArrowType.Decimal.createDecimal(Integer.parseInt(dataTypeDetails.get(1)), Integer.parseInt(dataTypeDetails.get(2)), 256);
            }

            if ("numeric".equalsIgnoreCase(dataType) || "float".equalsIgnoreCase(dataType) || "smallmoney".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.FLOAT8.getType();
            }

            if ("real".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.FLOAT4.getType();
            }

            if ("date".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.DATEDAY.getType();
            }

            if ("datetime".equalsIgnoreCase(dataType) || "datetime2".equalsIgnoreCase(dataType)
                    || "smalldatetime".equalsIgnoreCase(dataType) || "datetimeoffset".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.DATEMILLI.getType();
            }

            LOGGER.debug("columnType: " + columnType);
            schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
        }
        return schemaBuilder;
    }

    private SchemaBuilder doDataTypeConversionForNonCompatible(Connection jdbcConnection, TableName tableName, HashMap<String, List<String>> columnNameAndDataTypeMap) throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            boolean found = false;
            while (resultSet.next()) {
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                String columnName = resultSet.getString("COLUMN_NAME");
                String dataType = columnNameAndDataTypeMap.get(columnName).get(0);

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
        }
        return schemaBuilder;
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
}
