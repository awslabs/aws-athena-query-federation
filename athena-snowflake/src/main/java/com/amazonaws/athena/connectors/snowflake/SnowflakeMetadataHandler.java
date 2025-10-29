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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connector.util.PaginationHelper;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.resolver.SnowflakeJDBCCaseResolver;
import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.annotations.VisibleForTesting;
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
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.GetFunctionRequest;
import software.amazon.awssdk.services.lambda.model.GetFunctionResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.MAX_PARTITION_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SINGLE_SPLIT_LIMIT_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;

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
    public static final String SEPARATOR = "/";
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
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
    public static final String QUERY_ID = "queryId";
    public static final String PREPARED_STMT = "preparedStmt";
    private S3Client amazonS3;
    SnowflakeQueryStringBuilder snowflakeQueryStringBuilder = new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));
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
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SNOWFLAKE_NAME, configOptions), configOptions);
        this.amazonS3 = S3Client.create();
    }

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                new SnowflakeConnectionFactory(databaseConnectionConfig, SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS, SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)),
                configOptions);
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

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions, new SnowflakeJDBCCaseResolver(SNOWFLAKE_NAME));
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
        // Always ensure the partition column exists in the schema
        if (partitionSchemaBuilder.getField(BLOCK_PARTITION_COLUMN_NAME) == null) {
            partitionSchemaBuilder.addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        }
        partitionSchemaBuilder.addField(QUERY_ID, new ArrowType.Utf8());
        partitionSchemaBuilder.addField(PREPARED_STMT, new ArrowType.Utf8());
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
        String queryID = request.getQueryId();
        String catalog = request.getCatalogName();

        // Check if S3 export is enabled
        SnowflakeEnvironmentProperties envProperties = new SnowflakeEnvironmentProperties(System.getenv());
        
        if (envProperties.isS3ExportEnabled()) {
            handleS3ExportPartitions(blockWriter, request, schemaName, tableName, constraints, queryID, catalog);
        }
        else {
            handleDirectQueryPartitions(blockWriter, request, schemaName, tableName, constraints, queryID);
        }
    }

    private void handleDirectQueryPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, 
            Schema schemaName, TableName tableName, Constraints constraints, String queryID) throws Exception
    {
        LOGGER.debug("getPartitions: {}: Schema {}, table {}", queryID, tableName.getSchemaName(),
                tableName.getTableName());

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            /**
             * "MAX_PARTITION_COUNT" is currently set to 50 to limit the number of partitions.
             * this is to handle timeout issues because of huge partitions
             */
            LOGGER.info(" Total Partition Limit" + MAX_PARTITION_COUNT);
            boolean viewFlag = checkForView(tableName);
            //if the input table is a view , there will be single split
            if (viewFlag) {
                blockWriter.writeRows((Block block, int rowNum) -> {
                    block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
                    return 1;
                });
                return;
            }

            double totalRecordCount = 0;
            LOGGER.info(COUNT_RECORDS_QUERY);

            try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                    .withConnection(connection)
                    .withQuery(COUNT_RECORDS_QUERY)
                    .withParameters(Arrays.asList(tableName.getSchemaName(), tableName.getTableName())).build();
                 ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    totalRecordCount = rs.getLong(1);
                }
                if (totalRecordCount > 0) {
                    Optional<String> primaryKey = getPrimaryKey(tableName);
                    long recordsInPartition = (long) (Math.ceil(totalRecordCount / MAX_PARTITION_COUNT));
                    long partitionRecordCount = (totalRecordCount <= SINGLE_SPLIT_LIMIT_COUNT || !primaryKey.isPresent()) ? (long) totalRecordCount : recordsInPartition;
                    LOGGER.info(" Total Page Count: " + partitionRecordCount);
                    double numberOfPartitions = (int) Math.ceil(totalRecordCount / partitionRecordCount);
                    long offset = 0;
                    /**
                     * Custom pagination based partition logic will be applied with limit and offset clauses.
                     * It will have maximum 50 partitions and number of records in each partition is decided by dividing total number of records by 50
                     * the partition values we are setting the limit and offset values like p-limit-3000-offset-0
                     */
                    for (int i = 1; i <= numberOfPartitions; i++) {
                        final String partitionVal = BLOCK_PARTITION_COLUMN_NAME + "-primary-" + primaryKey.orElse("") + "-limit-" + partitionRecordCount + "-offset-" + offset;
                        LOGGER.info("partitionVal {} ", partitionVal);
                        blockWriter.writeRows((Block block, int rowNum) ->
                        {
                            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, partitionVal);
                            return 1;
                        });
                        offset = offset + partitionRecordCount;
                    }
                }
                else {
                    LOGGER.info("No Records Found for table {}", tableName);
                }
            }
        }
    }

    private void handleS3ExportPartitions(BlockWriter blockWriter, GetTableLayoutRequest request,
            Schema schemaName, TableName tableName, Constraints constraints, String queryID, String catalog) throws Exception
    {
        String s3ExportBucket = getS3ExportBucket();
        String randomStr = UUID.randomUUID().toString();
        // Sanitize and validate integration name to follow Snowflake naming rules
        String integrationName = catalog.concat(s3ExportBucket)
                .concat("_integration")
                .replaceAll("[^A-Za-z0-9_]", "_") // Replace any non-alphanumeric characters with underscore
                .replaceAll("_+", "_") // Replace multiple underscores with a single one
                .toUpperCase(); // Snowflake identifiers are case-insensitive and stored as uppercase

        // Validate integration name length and format
        if (integrationName.length() > 255) { // Snowflake's maximum identifier length
            throw new IllegalArgumentException("Integration name exceeds maximum length of 255 characters: " + integrationName);
        }
        if (!integrationName.matches("^[A-Z][A-Z0-9_]*$")) { // Must start with a letter
            throw new IllegalArgumentException("Invalid integration name format. Must start with a letter and contain only letters, numbers, and underscores: " + integrationName);
        }
        LOGGER.debug("Integration Name {}", integrationName);

        Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());

        // Check and create S3 integration if needed
        if (!checkIntegration(connection, integrationName)) {
            // Build the integration creation query with proper quoting and escaping
            String roleArn = getRoleArn(request.getContext());
            if (roleArn == null || roleArn.trim().isEmpty()) {
                throw new IllegalArgumentException("Role ARN cannot be null or empty");
            }
            
            String createIntegrationQuery = String.format(
                    "CREATE STORAGE INTEGRATION %s " +
                    "TYPE = EXTERNAL_STAGE " +
                    "STORAGE_PROVIDER = 'S3' " +
                    "ENABLED = TRUE " +
                    "STORAGE_AWS_ROLE_ARN = %s " +
                    "STORAGE_ALLOWED_LOCATIONS = (%s);",
                    snowflakeQueryStringBuilder.quote(integrationName),
                    snowflakeQueryStringBuilder.singleQuote(roleArn),
                    snowflakeQueryStringBuilder.singleQuote("s3://" + s3ExportBucket.replace("'", "''") + "/"));
            
            try (Statement stmt = connection.createStatement()) {
                LOGGER.debug("Create Integration query: {}", createIntegrationQuery);
                stmt.execute(createIntegrationQuery);
            }
            catch (SQLException e) {
                LOGGER.error("Failed to execute integration creation query: {}", createIntegrationQuery, e);
                throw new RuntimeException("Error creating integration: " + e.getMessage(), e);
            }
        }

        String generatedSql;
        if (constraints.isQueryPassThrough()) {
            generatedSql = buildQueryPassthroughSql(constraints);
        }
        else {
            generatedSql = snowflakeQueryStringBuilder.buildSqlString(connection, catalog, tableName.getSchemaName(), 
                    tableName.getTableName(), schemaName, constraints, null);
        }

        // Escape special characters in path components
        String escapedBucket = s3ExportBucket.replace("'", "''");
        String escapedQueryID = queryID.replace("'", "''");
        String escapedRandomStr = randomStr.replace("'", "''");
        String escapedIntegration = integrationName.replace("\"", "\"\"");
        
        // Build the COPY INTO query with proper escaping and quoting
        String s3Path = String.format("s3://%s/%s/%s/",
                escapedBucket.replace("'", "''"),
                escapedQueryID.replace("'", "''"),
                escapedRandomStr.replace("'", "''"));
                
        String snowflakeExportQuery = String.format("COPY INTO '%s' FROM (%s) STORAGE_INTEGRATION = %s " +
                "HEADER = TRUE FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY') MAX_FILE_SIZE = 16777216",
                s3Path,
                generatedSql,
                snowflakeQueryStringBuilder.quote(escapedIntegration));

        LOGGER.info("Snowflake Copy Statement: {} for queryId: {}", snowflakeExportQuery, queryID);

        blockWriter.writeRows((Block block, int rowNum) -> {
            boolean matched;
            matched = block.setValue(QUERY_ID, rowNum, queryID);
            matched &= block.setValue(PREPARED_STMT, rowNum, snowflakeExportQuery);
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
                if (existingIntegration != null) {
                    LOGGER.debug("Found integration: {}", existingIntegration);
                    // Normalize both names to uppercase for comparison
                    if (existingIntegration.trim().equalsIgnoreCase(integrationName.trim())) {
                        return true;
                    }
                }
            }
            LOGGER.debug("Integration {} not found", integrationName);
            return false;
        }
        catch (SQLException e) {
            LOGGER.error("Error checking for integration {}: {}", integrationName, e.getMessage());
            throw new SQLException("Failed to check for integration existence: " + e.getMessage(), e);
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        SnowflakeEnvironmentProperties envProperties = new SnowflakeEnvironmentProperties(System.getenv());
        
        if (envProperties.isS3ExportEnabled()) {
            return handleS3ExportSplits(request);
        }
        else {
            return handleDirectQuerySplits(request);
        }
    }

    private GetSplitsResponse handleDirectQuerySplits(GetSplitsRequest getSplitsRequest)
    {
        LOGGER.info("doGetSplits: {}: Catalog {}, table {}", getSplitsRequest.getQueryId(), getSplitsRequest.getTableName().getSchemaName(), getSplitsRequest.getTableName().getTableName());
        if (getSplitsRequest.getConstraints().isQueryPassThrough()) {
            LOGGER.info("QPT Split Requested");
            return setupQueryPassthroughSplit(getSplitsRequest);
        }
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

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.parseInt(request.getContinuationToken());
        }
        //No continuation token present
        return 0;
    }

    private GetSplitsResponse handleS3ExportSplits(GetSplitsRequest request)
    {
        Set<Split> splits = new HashSet<>();
        String exportBucket = getS3ExportBucket();
        String queryId = request.getQueryId();

        // Get the SQL statement which was created in getPartitions
        FieldReader fieldReaderQid = request.getPartitions().getFieldReader(QUERY_ID);
        String queryID = fieldReaderQid.readText().toString();

        FieldReader fieldReaderPreparedStmt = request.getPartitions().getFieldReader(PREPARED_STMT);
        String preparedStmt = fieldReaderPreparedStmt.readText().toString();

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder()
                     .withConnection(connection)
                     .withQuery(preparedStmt)
                     .withParameters(List.of(request.getTableName().getSchemaName() + "." +
                             request.getTableName().getTableName()))
                     .build()) {
            String prefix = queryId + SEPARATOR;
            List<S3Object> s3ObjectSummaries = getlistExportedObjects(exportBucket, prefix);
            LOGGER.debug("{} s3ObjectSummaries returned for queryId {}", (long) s3ObjectSummaries.size(), queryId);

            if (s3ObjectSummaries.isEmpty()) {
                preparedStatement.execute();
                s3ObjectSummaries = getlistExportedObjects(exportBucket, prefix);
                LOGGER.debug("{} s3ObjectSummaries returned after executing on SnowFlake for queryId {}", 
                        (long) s3ObjectSummaries.size(), queryId);
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
                return new GetSplitsResponse(request.getCatalogName(), splits);
            }
            else {
                Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add(SNOWFLAKE_SPLIT_QUERY_ID, queryID)
                        .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, exportBucket)
                        .add(SNOWFLAKE_SPLIT_OBJECT_KEY, EMPTY_STRING)
                        .build();
                splits.add(split);
                return new GetSplitsResponse(request.getCatalogName(), split);
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
            String errorMsg = String.format("Failed to list objects in bucket %s with prefix %s", s3ExportBucket, queryId);
            LOGGER.error("{}: {}", errorMsg, e.getMessage());
            throw new RuntimeException(errorMsg, e);
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

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        LOGGER.debug("getPartitionSchema: " + catalogName);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    private Optional<String> getPrimaryKey(TableName tableName) throws Exception
    {
        LOGGER.debug("getPrimaryKey tableName: " + tableName);
        List<String> primaryKeys = new ArrayList<String>();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(SHOW_PRIMARY_KEYS_QUERY + "\"" + tableName.getSchemaName() + "\".\"" + tableName.getTableName() + "\"");
                 ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    // Concatenate multiple primary keys if they exist
                    primaryKeys.add(rs.getString(PRIMARY_KEY_COLUMN_NAME));
                }
            }

            String primaryKeyString = primaryKeys.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
            if (!Strings.isNullOrEmpty(primaryKeyString) && hasUniquePrimaryKey(tableName, primaryKeyString)) {
                return Optional.of(primaryKeyString);
            }
        }
        return Optional.empty();
    }

    /**
     * Snowflake does not enforce primary key constraints, so we double-check user has unique primary key
     * before partitioning.
     */
    private boolean hasUniquePrimaryKey(TableName tableName, String primaryKey) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT " + primaryKey +  ", count(*) as COUNTS FROM " + "\"" + tableName.getSchemaName() + "\".\"" + tableName.getTableName() + "\"" + " GROUP BY " + primaryKey + " ORDER BY COUNTS DESC");
                 ResultSet rs = preparedStatement.executeQuery()) {
                if (rs.next()) {
                    if (rs.getInt(COUNTS_COLUMN_NAME) == 1) {
                        // Since it is in descending order and 1 is this first count seen,
                        // this table has a unique primary key
                        return true;
                    }
                }
            }
        }
        LOGGER.warn("Primary key ,{}, is not unique. Falling back to single partition...", primaryKey);
        return false;
    }

    /*
     * Check if the input table is a view and returns viewflag accordingly
     */
    private boolean checkForView(TableName tableName) throws Exception
    {
        boolean viewFlag = false;
        List<String> viewparameters = Arrays.asList(tableName.getSchemaName(), tableName.getTableName());
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
            throw new RuntimeException("Error fetching IAM role ARN: " + e.getMessage(), e);
        }
    }

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        final String secretName = getDatabaseConnectionConfig().getSecret();
        if (StringUtils.isNotBlank(secretName)) {
            return new SnowflakeCredentialsProvider(secretName);
        }

        return null;
    }
}
