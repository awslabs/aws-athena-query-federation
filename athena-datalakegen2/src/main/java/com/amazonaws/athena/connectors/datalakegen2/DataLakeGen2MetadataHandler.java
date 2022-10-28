/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataLakeGen2MetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLakeGen2MetadataHandler.class);

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String PARTITION_NUMBER = "PARTITION_NUMBER";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link DataLakeGen2MuxCompositeHandler} instead.
     */
    public DataLakeGen2MetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DataLakeGen2Constants.NAME));
    }

    /**
     * Used by Mux.
     */
    public DataLakeGen2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new DataLakeGen2JdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(DataLakeGen2Constants.DRIVER_CLASS, DataLakeGen2Constants.DEFAULT_PORT)));
    }

    public DataLakeGen2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }

    @VisibleForTesting
    protected DataLakeGen2MetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
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
     * The partitions are being implemented based on the type of data externally in case of Gen 2.
     * Considering the ADLS Gen2 data has already been partitioned and distributed within Gen 2 storage system, connector will fetch data as single split.
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     * @throws Exception
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker)
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());

        blockWriter.writeRows((Block block, int rowNum) ->
        {
            LOGGER.debug("Getting Data ");
            block.setValue(PARTITION_NUMBER, rowNum, "0");
            //we wrote 1 row so we return 1
            return 1;
        });
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());

        // Always create single split
        Set<Split> splits = new HashSet<>();
        splits.add(Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
                .add(PARTITION_NUMBER, "0").build());
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

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
            stmt.setString(1, tableName.getSchemaName().toUpperCase() + "." + tableName.getTableName().toUpperCase());
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
                 * TINYINT range is 0 to 255 in SQL Server, usage of TINYINT(ArrowType) leads to data loss as its using 1 bit as signed flag.
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
}
