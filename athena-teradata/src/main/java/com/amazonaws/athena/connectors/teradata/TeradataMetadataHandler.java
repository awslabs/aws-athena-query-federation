
/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;
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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.DateUnit;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles metadata for Teradata. User must have access to `schemata`, `tables`, `columns`, `partitions` tables in
 * information_schema.
 */
public class TeradataMetadataHandler extends JdbcMetadataHandler
{
    static final String ALL_PARTITIONS = "*";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";

    private static final Logger LOGGER = LoggerFactory.getLogger(TeradataMetadataHandler.class);
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    /**
     * Query for retrieving view info Teradata
     */
    static final String VIEW_CHECK_QUERY = "SELECT * FROM dbc.Tables WHERE UPPER(DatabaseName) = UPPER(?)  and tablekind='V' and UPPER(TableName)=UPPER(?) ";
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link TeradataMuxCompositeHandler} instead.
     */
    public TeradataMetadataHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(TeradataConstants.TERADATA_NAME));
    }

    /**
     * Used by Mux.
     */
    public TeradataMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
       this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig, null, new DatabaseConnectionInfo(TeradataConstants.TERADATA_DRIVER_CLASS, TeradataConstants.TERADATA_DEFAULT_PORT)));
    }

    public TeradataMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory);
    }

    @VisibleForTesting
    protected TeradataMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AWSSecretsManager secretsManager,
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
    /*
     * We are first checking if input table is a view, if it's a view, it will not have any partition info and
     * data will be fetched with single split.If it is a table with no partition, then data will be fetched with single split.
     * If it is a partitioned table, we are fetching the partition info and creating splits equals to the number of partitions
     * for parallel processing.
     * Teradata partitions
     * @param blockWriter
     * @param getTableLayoutRequest
     * @param queryStatusChecker
     * @throws Exception
     *
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest getTableLayoutRequest,
                              QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        LOGGER.info("{}: Schema {}, table {}", getTableLayoutRequest.getQueryId(), getTableLayoutRequest.getTableName().getSchemaName(),
                getTableLayoutRequest.getTableName().getTableName());
        final String getPartitionsQuery = "Select DISTINCT partition FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        boolean viewFlag = false;
        //Check if input table is a view
        List<String> viewparameters = Arrays.asList(getTableLayoutRequest.getTableName().getSchemaName(), getTableLayoutRequest.getTableName().getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(VIEW_CHECK_QUERY).withParameters(viewparameters).build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    viewFlag = true;
                }
                LOGGER.debug("viewFlag: {}", viewFlag);
            }
        }
        //if the input table is a view , there will be single split
        if (viewFlag) {
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                return 1;
            });
        }
        else {
             /*
             It is not a view, check if query should be executed with single split by reading environment variable partitioncount
             partitioncount is a configurable Environment variable which has been defined. It limits maximum number of
             partitions, if it exceeds the value, then there will be only single split. This use case has been added to handle scenario
             where there are huge partitions and query times out. If appropriate predicate filter is applied , then data will be fetched
             without query getting timed out.
            */
            boolean nonPartitionApproach = useNonPartitionApproach(getTableLayoutRequest);
            if (nonPartitionApproach) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    return 1;
                });
            }
            else {
                List<String> parameters = Arrays.asList(Integer.toString(1));
                try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
                    getPartitionDetails(blockWriter, getPartitionsQuery, parameters, connection);
                }
            }
        }
    }

    /**
     * Internal function for non-partition Approach
     * Configurable Environment variable partitioncount has been defined which limits maximum number of
     * partitions, if it exceeds the value, then there will be only single split.
     * @param getTableLayoutRequest
     * @return
     * @throws Exception
     */
    private boolean useNonPartitionApproach(GetTableLayoutRequest getTableLayoutRequest) throws Exception
    {
        final String getPartitionsCountQuery = "Select  count(distinct partition ) as partition_count FROM " + getTableLayoutRequest.getTableName().getSchemaName() + "." +
                getTableLayoutRequest.getTableName().getTableName() + " where 1= ?";
        String partitioncount = System.getenv().get("partitioncount");
        int totalPartitionCount = Integer.parseInt(partitioncount);
        int  partitionCount = 0;
        boolean nonPartitionApproach = false;
        List<String> params = Arrays.asList(Integer.toString(1));
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(getPartitionsCountQuery).withParameters(params).build();
                 ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    partitionCount = Integer.parseInt(resultSet.getString("partition_count"));
                }
                if (partitionCount > totalPartitionCount) {
                    nonPartitionApproach = true;
                }
                LOGGER.info("nonPartitionApproach: {}", nonPartitionApproach);
            }
        }
        return nonPartitionApproach;
    }
    /**
     * Internal function to fetch partition details
     * @param blockWriter
     * @param getPartitionsQuery
     * @param parameters
     * @param connection
     * @throws SQLException
     */
    private void getPartitionDetails(BlockWriter blockWriter, String getPartitionsQuery, List<String> parameters, Connection connection) throws SQLException
    {
        try (PreparedStatement preparedStatement = new PreparedStatementBuilder().withConnection(connection).withQuery(getPartitionsQuery).withParameters(parameters).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            // Return a single partition if no partitions defined
            if (!resultSet.next()) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
            else {
                do {
                    final String partitionName = resultSet.getString(BLOCK_PARTITION_COLUMN_NAME);

                    // 1. Returns all partitions of table, we are not supporting constraints push down to filter partitions.
                    // 2. This API is not paginated, we could use order by and limit clause with offsets here.
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionName);
                        //we wrote 1 row so we return 1
                        return 1;
                    });
                }
                while (resultSet.next());
            }
        }
        catch (RuntimeException runtimeException) {
            LOGGER.info("Exception occurred: {}", runtimeException.getMessage());
            if (runtimeException.getMessage().equalsIgnoreCase("Invalid Partition field.")) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    //we wrote 1 row so we return 1
                    return 1;
                });
            }
        }
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
            return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), getSchema(connection, getTableRequest.getTableName(), partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
        }
    }

    /**
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws SQLException
     */
    private Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            boolean found = false;
            while (resultSet.next()) {
                ArrowType columnType = toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"));
                String columnName = resultSet.getString("COLUMN_NAME");

                LOGGER.info("Column Name: " + columnName);
                if (columnType != null) {
                    LOGGER.info("Column Type: " + columnType.getTypeID());
                }
                else {
                    LOGGER.warn("Column Type is null for Column Name" + columnName);
                }

                /**
                 * Convert decimal into BigInt
                 */
                if (columnType != null && columnType.getTypeID().equals(ArrowType.ArrowTypeID.Decimal)) {
                    String[] data = columnType.toString().split(",");
                    if (data[0].contains("0") || data[1].contains("0")) {
                        columnType = org.apache.arrow.vector.types.Types.MinorType.BIGINT.getType();
                    }
                }
                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    if (columnType instanceof ArrowType.List) {
                        schemaBuilder.addListField(columnName, getArrayArrowTypeFromTypeName(
                                resultSet.getString("TYPE_NAME"),
                                resultSet.getInt("COLUMN_SIZE"),
                                resultSet.getInt("DECIMAL_DIGITS")));
                    }
                    else {
                        LOGGER.info("getSchema:columnType is not instance of ArrowType column[" + columnName +
                                "] to a supported type, attempted " + columnType + " - defaulting type to VARCHAR.");
                        schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                    }
                }
                else {
                    // Default to VARCHAR ArrowType
                    LOGGER.info("getSchema: Unable to map type for column[" + columnName +
                            "] to a supported type, attempted " + columnType + " - defaulting type to VARCHAR.");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, new ArrowType.Utf8()).build());
                }
                found = true;
            }

            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }
            // add partition columns
            partitionSchema.getFields().forEach(schemaBuilder::addField);

            return schemaBuilder.build();
        }
    }
    public static ArrowType toArrowType(final int jdbcType, final int precision, final int scale)
    {
        ArrowType arrowType = JdbcToArrowUtils.getArrowTypeFromJdbcType(
                new JdbcFieldInfo(jdbcType, precision, scale),
                null);
        if (arrowType instanceof ArrowType.Date) {
            // Convert from DateMilli to DateDay
            return new ArrowType.Date(DateUnit.DAY);
        }
        return arrowType;
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
