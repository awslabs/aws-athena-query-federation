
/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

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
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.Set;
import java.util.stream.Collectors;
/**
 * Handles metadata for ORACLE. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class OracleMetadataHandler
        extends JdbcMetadataHandler
{
    static final String GET_PARTITIONS_QUERY = "Select DISTINCT PARTITION_NAME FROM USER_TAB_PARTITIONS where table_name= ?";
    static final String BLOCK_PARTITION_COLUMN_NAME = "PARTITION_NAME";
    static final String ALL_PARTITIONS = "0";
    static final String PARTITION_COLUMN_NAME = "PARTITION_NAME";
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private static final String COLUMN_NAME = "COLUMN_NAME";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link OracleMuxCompositeHandler} instead.
     */
    public OracleMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(OracleConstants.ORACLE_NAME));
    }

    /**
     * Used by Mux.
     */
    public OracleMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new OracleJdbcConnectionFactory(databaseConnectionConfig, new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS, OracleConstants.ORACLE_DEFAULT_PORT)));
    }

    public OracleMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }

    @VisibleForTesting
    protected OracleMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
                                    AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     *
     * If it is a table with no partition, then data will be fetched with single split.
     * If it is a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
     * for parallel processing.
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     */
    @Override
    public void getPartitions(final BlockWriter blockWriter, final GetTableLayoutRequest getTableLayoutRequest, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.debug("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
          List<String> parameters = Arrays.asList(getTableLayoutRequest.getTableName().getTableName().toUpperCase());
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
                            LOGGER.debug("Adding partition {}", partitionName);
                            //we wrote 1 row so we return 1
                            return 1;
                        });
                    }
                    while (resultSet.next() && queryStatusChecker.isQueryRunning());
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
    public GetSplitsResponse doGetSplits(
            final BlockAllocator blockAllocator, final GetSplitsRequest getSplitsRequest)
    {
        LOGGER.debug("{}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
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
            TableName tableName = new TableName(getTableRequest.getTableName().getSchemaName().toUpperCase(), getTableRequest.getTableName().getTableName().toUpperCase());
            return new GetTableResponse(getTableRequest.getCatalogName(), tableName, getSchema(connection, tableName, partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
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

    protected String escapeNamePattern(final String name, final String escape)
    {
        if ((name == null) || (escape == null)) {
            return name;
        }
        Preconditions.checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        Preconditions.checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        String escapedName = name.replace(escape, escape + escape);
        escapedName = escapedName.replace("_", escape + "_");
        escapedName = escapedName.replace("%", escape + "%");
        return escapedName;
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
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            boolean found = false;
            HashMap<String, String> hashMap = new HashMap<String, String>();
            /**
             * Getting original data type from oracle table for conversion
             */
            try
                    (PreparedStatement stmt = connection.prepareStatement("select COLUMN_NAME ,DATA_TYPE from USER_TAB_COLS where  table_name =?")) {
                stmt.setString(1, tableName.getTableName().toUpperCase());
                ResultSet dataTypeResultSet = stmt.executeQuery();
                while (dataTypeResultSet.next()) {
                    hashMap.put(dataTypeResultSet.getString(COLUMN_NAME).trim(), dataTypeResultSet.getString("DATA_TYPE").trim());
                }
                while (resultSet.next()) {
                    ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    String columnName = resultSet.getString(COLUMN_NAME);
                    /** Handling TIMESTAMP,DATE, 0 Precesion**/
                    if (columnType != null && columnType.getTypeID().equals(ArrowType.ArrowTypeID.Decimal)) {
                        String[] data = columnType.toString().split(",");
                        if (data[0].contains("0") || data[1].contains("0")) {
                            columnType = Types.MinorType.BIGINT.getType();
                        }

                        /** Handling negative scale issue */
                        if (Integer.parseInt(data[1].trim().replace(")", "")) < 0.0) {
                            columnType = Types.MinorType.VARCHAR.getType();
                        }
                    }

                    String dataType = hashMap.get(columnName);
                    LOGGER.debug("columnName: " + columnName);
                    LOGGER.debug("dataType: " + dataType);
                    /**
                     * below data type conversion  doing since framework not giving appropriate
                     * data types for oracle data types..
                     */
                    /**
                     * Converting oracle date data type into DATEDAY MinorType
                     */
                    if (dataType != null && (dataType.contains("date") || dataType.contains("DATE"))) {
                        columnType = Types.MinorType.DATEDAY.getType();
                    }
                    /**
                     * Converting oracle NUMBER data type into BIGINT  MinorType
                     */
                    if (dataType != null && (dataType.contains("NUMBER")) && columnType.getTypeID().toString().equalsIgnoreCase("Utf8")) {
                        columnType = Types.MinorType.BIGINT.getType();
                    }

                    /**
                     * Converting oracle TIMESTAMP data type into DATEMILLI  MinorType
                     */
                    if (dataType != null && (dataType.contains("TIMESTAMP"))
                    ) {
                        columnType = Types.MinorType.DATEMILLI.getType();
                    }
                    if (columnType == null) {
                        columnType = Types.MinorType.VARCHAR.getType();
                    }
                    if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                        columnType = Types.MinorType.VARCHAR.getType();
                    }

                    if (columnType != null && SupportedTypes.isSupported(columnType)) {
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                        found = true;
                    }
                    else {
                        LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                    }
                }
            }
            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }
            partitionSchema.getFields().forEach(schemaBuilder::addField);
            LOGGER.debug("Oracle Table Schema" + schemaBuilder.toString());
            return schemaBuilder.build();
        }
    }
}
