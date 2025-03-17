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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.athena.connectors.jdbc.resolver.JDBCCaseResolver;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.resolver.SnowflakeJDBCCaseResolver;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionRequest;
import software.amazon.awssdk.services.lambda.model.GetFunctionResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.MAX_PARTITION_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SINGLE_SPLIT_LIMIT_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_NAME;

/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns` in
 * information_schema.
 */
public class SnowflakeMetadataHandler extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA", "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE", "true");
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String EMPTY_STRING = StringUtils.EMPTY;
    public static final String SNOWFLAKE_EXPORT_PREFIX = "snowflake_data";
    public static final String SEPARATOR = "/";
    private S3Client amazonS3;
    SnowflakeQueryStringBuilder snowflakeQueryStringBuilder = new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));
    static final String LIST_PAGINATED_TABLES_QUERY =
            "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = ? " +
                    "ORDER BY TABLE_NAME " +
                    "LIMIT ? OFFSET ?";
    /**
     * fetching number of records in the table
     */
    static final String COUNT_RECORDS_QUERY = "SELECT row_count\n" +
            "FROM   information_schema.tables\n" +
            "WHERE  table_type = 'BASE TABLE'\n" +
            "AND table_schema= ?\n" +
            "AND TABLE_NAME = ? ";
    static final String SHOW_PRIMARY_KEYS_QUERY = "SHOW PRIMARY KEYS IN ";
    static final String PRIMARY_KEY_COLUMN_NAME = "column_name";
    static final String COUNTS_COLUMN_NAME = "COUNTS";
    /**
     * Query to check view
     */
    static final String VIEW_CHECK_QUERY = "SELECT * FROM information_schema.views WHERE table_schema = ? AND table_name = ?";
    static final String ALL_PARTITIONS = "*";

    static final Map<String, ArrowType> STRING_ARROW_TYPE_MAP = com.google.common.collect.ImmutableMap.of(
            "INTEGER", (ArrowType) Types.MinorType.INT.getType(),
            "DATE", (ArrowType) Types.MinorType.DATEDAY.getType(),
            "TIMESTAMP", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_LTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_NTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMP_TZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPLTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPNTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
            "TIMESTAMPTZ", (ArrowType) Types.MinorType.DATEMILLI.getType()
    );
    /**
     * Instantiates handler to be used by Lambda function directly.
     * <p>
     */
    public SnowflakeMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
        this.amazonS3 = S3Client.create();
    }

    /**
     * Used by Mux.
     */
    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                new SnowflakeConnectionFactory(databaseConnectionConfig, SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS, SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)),
                configOptions);
    }

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, new SnowflakeJDBCCaseResolver(SNOWFLAKE_NAME));
    }

    @VisibleForTesting
    protected SnowflakeMetadataHandler(
            DatabaseConnectionConfig databaseConnectionConfig,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            S3Client s3Client,
            JdbcConnectionFactory jdbcConnectionFactory,
            java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
        this.amazonS3 = s3Client;
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        LOGGER.debug("doGetDataSourceCapabilities: " + request);
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
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
    }

    /**
     * Here we inject the additional column to hold the Prepared SQL Statement.
     *
     * @param partitionSchemaBuilder The SchemaBuilder you can use to add additional columns and metadata to the
     *                               partitions response.
     * @param request                The GetTableLayoutResquest that triggered this call.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        LOGGER.info("{}: Catalog {}, table {}", request.getQueryId(), request.getTableName().getSchemaName(), request.getTableName());
        partitionSchemaBuilder.addField("queryId", new ArrowType.Utf8());
        partitionSchemaBuilder.addField("preparedStmt", new ArrowType.Utf8());
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     * Here generating the SQL from the request and attaching it as a additional column
     *
     * @param blockWriter        Used to write rows (partitions) into the Apache Arrow response.
     * @param request            Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception
    {
        Schema schemaName = request.getSchema();
        TableName tableName = request.getTableName();
        Constraints constraints = request.getConstraints();
        //get the bucket where export results wll be uploaded
        String s3ExportBucket = getS3ExportBucket();
        //Appending a random int to the query id to support multiple federated queries within a single query

        String randomStr = UUID.randomUUID().toString();
        String queryID = request.getQueryId().replace("-", "").concat(randomStr);
        String catalog = request.getCatalogName();
        String integrationName = catalog.concat(s3ExportBucket).concat("_integration").replaceAll("-", "_").replaceAll(":", "");
        LOGGER.debug("Integration Name {}", integrationName);
        //Build the SQL query
        Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());

        //Check if integration exists using SHOW INTEGRATIONS
        if (!checkIntegration(connection, integrationName)) {
            //Create integration if it does not exist
            String createIntegrationQuery = "CREATE STORAGE INTEGRATION " + integrationName + " " +
                    "TYPE = EXTERNAL_STAGE " +
                    "STORAGE_PROVIDER = 'S3' " +
                    "ENABLED = TRUE " +
                    "STORAGE_AWS_ROLE_ARN = '" + getRoleArn(request.getContext())  + "' " +
                    "STORAGE_ALLOWED_LOCATIONS = ('s3://" + s3ExportBucket + SEPARATOR + SNOWFLAKE_EXPORT_PREFIX + "/');";
            try (Statement stmt = connection.createStatement()) {
                LOGGER.debug("Create Integration {}", createIntegrationQuery);
                stmt.execute(createIntegrationQuery);
            }
        }

        String generatedSql;
        if (constraints.isQueryPassThrough()) {
            generatedSql = buildQueryPassthroughSql(constraints);
        }
        else {
            generatedSql = snowflakeQueryStringBuilder.buildSqlString(connection, catalog, tableName.getSchemaName(), tableName.getTableName(), schemaName, constraints, null);
        }
        String snowflakeExportQuery = "COPY INTO 's3://" + s3ExportBucket + SEPARATOR + SNOWFLAKE_EXPORT_PREFIX + SEPARATOR + queryID + "/' " +
                "FROM (" + generatedSql + ") " +
                "STORAGE_INTEGRATION = " + integrationName + " " +
                "HEADER = TRUE FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY') " +
                "MAX_FILE_SIZE = 16777216";

        LOGGER.info("Snowflake Copy Statement: {}", snowflakeExportQuery);
        LOGGER.info("queryID: {}", queryID);

        // write the prepared SQL statement to the partition column created in enhancePartitionSchema
        blockWriter.writeRows((Block block, int rowNum) -> {
            boolean matched;
            matched = block.setValue("queryId", rowNum, queryID);
            matched &= block.setValue("preparedStmt", rowNum, snowflakeExportQuery);
            //If all fields matches then we wrote 1 row during this call so we return 1
            return matched ? 1 : 0;
        });
    }

    private String buildQueryPassthroughSql(Constraints constraints)
    {
        jdbcQueryPassthrough.verify(constraints.getQueryPassthroughArguments());
        return  constraints.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
    }

    static boolean checkIntegration(Connection connection, String integrationName) throws SQLException
    {
        String checkIntegrationQuery = "SHOW INTEGRATIONS";
        ResultSet rs;
        try (Statement stmt = connection.createStatement()) {
            rs = stmt.executeQuery(checkIntegrationQuery);
            while (rs.next()) {
                String existingIntegration = rs.getString("name");
                LOGGER.debug("Integration {}", existingIntegration);
                // check integration name ignoring case-sensitivity
                if (existingIntegration.equalsIgnoreCase(integrationName)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        Set<Split> splits = new HashSet<>();
        String exportBucket = getS3ExportBucket();
        String queryId = request.getQueryId().replace("-", "");
        String catalogName = request.getCatalogName();

        // Get the SQL statement which was created in getPartitions
        FieldReader fieldReaderQid = request.getPartitions().getFieldReader("queryId");
        String queryID = fieldReaderQid.readText().toString();

        FieldReader fieldReaderPreparedStmt = request.getPartitions().getFieldReader("preparedStmt");
        String preparedStmt = fieldReaderPreparedStmt.readText().toString();

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder()
                     .withConnection(connection)
                     .withQuery(preparedStmt)
                     .withParameters(Arrays.asList(request.getTableName().getSchemaName() + "." +
                             request.getTableName().getTableName()))
                     .build()) {
            /*
             * For each generated S3 object, create a split and add data to the split.
             */
            String prefix = SNOWFLAKE_EXPORT_PREFIX + SEPARATOR + queryId;
            List<S3Object> s3ObjectSummaries = getlistExportedObjects(exportBucket, prefix);
            LOGGER.debug("s3ObjectSummaries {}", (long) s3ObjectSummaries.size());
            if (s3ObjectSummaries.isEmpty()) {
                // Execute queries on snowflake if S3 export bucket does not contain objects for given queryId
                preparedStatement.execute();
                // Retrieve the S3 objects list for given queryId
                s3ObjectSummaries = getlistExportedObjects(exportBucket, prefix);
                LOGGER.debug("s3ObjectSummaries2 {}", (long) s3ObjectSummaries.size());
            }

            if (!s3ObjectSummaries.isEmpty()) {
                for (S3Object objectSummary : s3ObjectSummaries) {
                    Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                            .add(SNOWFLAKE_SPLIT_QUERY_ID, queryID)
                            .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, exportBucket)
                            .add(SNOWFLAKE_SPLIT_OBJECT_KEY, objectSummary.key())
                            .build();
                    splits.add(split);
                }
                LOGGER.info("doGetSplits: exit - ", splits.size());
                return new GetSplitsResponse(catalogName, splits);
            }
            else {
                // No records were exported by Snowflake for the issued query, creating an "empty" split
                LOGGER.info("No records were exported by Snowflake");
                Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add(SNOWFLAKE_SPLIT_QUERY_ID, queryID)
                        .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, exportBucket)
                        .add(SNOWFLAKE_SPLIT_OBJECT_KEY, EMPTY_STRING)
                        .build();
                splits.add(split);
                LOGGER.info("doGetSplits: exit - ", splits.size());
                return new GetSplitsResponse(catalogName, split);
            }
        }
        catch (Exception throwables) {
            throw new RuntimeException("Exception in execution export statement " + throwables.getMessage(), throwables);
        }
    }

    @Override
    public ListTablesResponse listPaginatedTables(final Connection connection, final ListTablesRequest listTablesRequest) throws SQLException
    {
        LOGGER.debug("Starting listPaginatedTables for Snowflake.");
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

    /*
     * Get the list of all the exported S3 objects
     */
    private List<S3Object> getlistExportedObjects(String s3ExportBucket, String queryId)
    {
        ListObjectsResponse listObjectsResponse;
        try {
            listObjectsResponse = amazonS3.listObjects(ListObjectsRequest.builder()
                    .bucket(s3ExportBucket)
                    .prefix(queryId)
                    .build());
        }
        catch (SdkClientException e) {
            throw new RuntimeException("Exception listing the exported objects : " + e.getMessage(), e);
        }
        return listObjectsResponse.contents();
    }

    /**
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    private Schema getSchema(Connection jdbcConnection, TableName tableName)
    @Override
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        LOGGER.debug("getSchema start, tableName:" + tableName);
        /**
         * query to fetch column data type to handle appropriate datatype to arrowtype conversions.
         */
        String dataTypeQuery = "select COLUMN_NAME, DATA_TYPE from \"INFORMATION_SCHEMA\".\"COLUMNS\" WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            stmt.setString(1, tableName.getSchemaName());
            stmt.setString(2, tableName.getTableName());

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
                Optional<ArrowType> columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);
                String columnName = resultSet.getString(COLUMN_NAME);
                String dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);
                if (dataType != null && STRING_ARROW_TYPE_MAP.containsKey(dataType.toUpperCase())) {
                    columnType = Optional.of(STRING_ARROW_TYPE_MAP.get(dataType.toUpperCase()));
                }
                /**
                 * converting into VARCHAR for not supported data types.
                 */
                if (columnType.isEmpty()) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }
                if (columnType.isPresent() && !SupportedTypes.isSupported(columnType.get())) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                if (columnType.isPresent() && SupportedTypes.isSupported(columnType.get())) {
                    LOGGER.debug(" AddField Schema Building...()  ");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            }
            if (!found) {
                throw new AthenaConnectorException("Could not find table in " + tableName.getSchemaName(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString()).build());
            }
            partitionSchema.getFields().forEach(schemaBuilder::addField);
        }
        catch (SnowflakeSQLException ex) {
            throw new AthenaConnectorException(ex.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION.toString()).build());
        }
        LOGGER.debug(schemaBuilder.toString());
        return schemaBuilder.build();
    }

    @Override
    protected Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws SQLException
    {
        try (ResultSet resultSet = jdbcConnection
                .getMetaData()
                .getSchemas(jdbcConnection.getCatalog(), null)) {
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

    public String getS3ExportBucket()
    {
        return configOptions.get(SPILL_BUCKET_ENV);
    }

    public String getRoleArn(Context context)
    {
        String functionName = context.getFunctionName(); // Get the Lambda function name dynamically

        try (LambdaClient lambdaClient = LambdaClient.create()) {
            GetFunctionRequest request = GetFunctionRequest.builder()
                    .functionName(functionName)
                    .build();

            GetFunctionResponse response = lambdaClient.getFunction(request);
            return response.configuration().role();
        }
        catch (Exception e) {
            throw new RuntimeException("Error fetching IAM role ARN: " + e.getMessage());
        }
    }
}
