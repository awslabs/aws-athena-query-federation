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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.CredentialsProviderFactory;
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
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.datalakegen2.resolver.DataLakeGen2CaseResolver;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;

public class DataLakeGen2MetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataLakeGen2MetadataHandler.class);

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String PARTITION_NUMBER = "partition_number";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link DataLakeGen2MuxCompositeHandler} instead.
     */
    public DataLakeGen2MetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DataLakeGen2Constants.NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public DataLakeGen2MetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES,
                new DatabaseConnectionInfo(DataLakeGen2Constants.DRIVER_CLASS, DataLakeGen2Constants.DEFAULT_PORT)),
                configOptions);
    }

    public DataLakeGen2MetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME));
    }

    @VisibleForTesting
    protected DataLakeGen2MetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions,
        JDBCCaseResolver caseResolver)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions, caseResolver);
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        Set<StandardFunctions> unSupportedFunctions = ImmutableSet.of(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME);
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();

        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .filter(values -> !unSupportedFunctions.contains(values))
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
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
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }
        // Always create single split
        Set<Split> splits = new HashSet<>();
        splits.add(Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
                .add(PARTITION_NUMBER, "0").build());
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), splits, null);
    }

    @Override
    protected Optional<ArrowType> convertDatasourceTypeToArrow(int columnIndex, int precision, Map<String, String> configOptions, ResultSetMetaData metadata) throws SQLException
    {
        String dataType = metadata.getColumnTypeName(columnIndex);
        LOGGER.info("In convertDatasourceTypeToArrow: converting {}", dataType);
        if (dataType != null && DataLakeGen2DataType.isSupported(dataType)) {
            LOGGER.debug("Data lake Gen2 Datatype is support: {}", dataType);
            return Optional.of(DataLakeGen2DataType.fromType(dataType));
        }
        return super.convertDatasourceTypeToArrow(columnIndex, precision, configOptions, metadata);
    }

    /**
     * Appropriate datatype to arrow type conversions will be done by fetching data types of columns
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
        LOGGER.info("Inside getSchema");

        String dataTypeQuery = "SELECT C.NAME AS COLUMN_NAME, TYPE_NAME(C.USER_TYPE_ID) AS DATA_TYPE, " +
                "C.PRECISION, C.SCALE " +
                "FROM sys.columns C " +
                "JOIN sys.types T " +
                "ON C.USER_TYPE_ID=T.USER_TYPE_ID " +
                "WHERE C.OBJECT_ID=OBJECT_ID(?)";

        String dataType;
        String columnName;
        int precision;
        int scale;
        HashMap<String, ColumnInfo> hashMap = new HashMap<>();

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            // fetch data types of columns and prepare map with column name and datatype.
            stmt.setString(1, tableName.getSchemaName() + "." + tableName.getTableName());
            try (ResultSet dataTypeResultSet = stmt.executeQuery()) {
                while (dataTypeResultSet.next()) {
                    dataType = dataTypeResultSet.getString("DATA_TYPE");
                    columnName = dataTypeResultSet.getString("COLUMN_NAME");
                    precision = dataTypeResultSet.getInt("PRECISION");
                    scale = dataTypeResultSet.getInt("SCALE");
                    hashMap.put(columnName.trim(), new ColumnInfo(dataType.trim(), precision, scale));
                }
            }
        }

        String environment = DataLakeGen2Util.checkEnvironment(jdbcConnection.getMetaData().getURL());
        
        if (DataLakeGen2Constants.SQL_POOL.equalsIgnoreCase(environment)) {
            // getColumns() method from SQL Server driver is causing an exception in case of Azure Serverless environment.
            // so doing explicit data type conversion
            schemaBuilder = doDataTypeConversion(hashMap);
        }
        else {
            schemaBuilder = doDataTypeConversionForNonCompatible(jdbcConnection, tableName, hashMap);
        }
        // add partition columns
        partitionSchema.getFields().forEach(schemaBuilder::addField);
        return schemaBuilder.build();
    }

    private SchemaBuilder doDataTypeConversion(HashMap<String, ColumnInfo> columnNameAndDataTypeMap)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        for (Map.Entry<String, ColumnInfo> entry : columnNameAndDataTypeMap.entrySet()) {
            String columnName = entry.getKey();
            ColumnInfo columnInfo = entry.getValue();
            String dataType = columnInfo.getDataType();
            ArrowType columnType = Types.MinorType.VARCHAR.getType();

            if ("char".equalsIgnoreCase(dataType) || "varchar".equalsIgnoreCase(dataType) ||
                    "nchar".equalsIgnoreCase(dataType) || "nvarchar".equalsIgnoreCase(dataType)
                    || "time".equalsIgnoreCase(dataType) || "uniqueidentifier".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.VARCHAR.getType();
            }

            if ("binary".equalsIgnoreCase(dataType) || "varbinary".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.VARBINARY.getType();
            }

            if ("bit".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.BIT.getType();
            }

            if ("tinyint".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.TINYINT.getType();
            }

            if ("smallint".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.SMALLINT.getType();
            }

            if ("int".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.INT.getType();
            }

            if ("bigint".equalsIgnoreCase(dataType)) {
                columnType = Types.MinorType.BIGINT.getType();
            }

            if ("decimal".equalsIgnoreCase(dataType)) {
                columnType = new ArrowType.Decimal(columnInfo.getPrecision(), columnInfo.getScale(), 128);
            }

            if ("numeric".equalsIgnoreCase(dataType) || "float".equalsIgnoreCase(dataType) || "smallmoney".equalsIgnoreCase(dataType) || "money".equalsIgnoreCase(dataType)) {
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

            schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
        }
        return schemaBuilder;
    }

    private SchemaBuilder doDataTypeConversionForNonCompatible(Connection jdbcConnection, TableName tableName, HashMap<String, ColumnInfo> columnNameAndDataTypeMap) throws SQLException
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            boolean found = false;
            while (resultSet.next()) {
                Optional<ArrowType> columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);
                String columnName = resultSet.getString("COLUMN_NAME");
                ColumnInfo columnInfo = columnNameAndDataTypeMap.get(columnName);

                if (columnInfo != null && DataLakeGen2DataType.isSupported(columnInfo.getDataType())) {
                    columnType = Optional.of(DataLakeGen2DataType.fromType(columnInfo.getDataType()));
                }

                /**
                 * converting into VARCHAR for non supported data types.
                 */
                if (columnType.isEmpty() || !SupportedTypes.isSupported(columnType.get())) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                if (columnType.isPresent() && SupportedTypes.isSupported(columnType.get())) {
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[{}] to a supported type, attempted {}", columnName, columnType);
                }
            }
            if (!found) {
                LOGGER.error("Could not find any supported columns in table: {}.{}", tableName.getSchemaName(), tableName.getTableName());
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }
        }
        return schemaBuilder;
    }

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        return CredentialsProviderFactory.createCredentialProvider(
                getDatabaseConnectionConfig().getSecret(),
                getCachableSecretsManager(),
                new DataLakeGen2OAuthCredentialsProvider()
        );
    }
}

class ColumnInfo
{
    private final String dataType;
    private final int precision;
    private final int scale;

    public ColumnInfo(String dataType, int precision, int scale)
    {
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
    }

    public String getDataType()
    {
        return dataType;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }
}
