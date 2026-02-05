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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.resolver.SnowflakeJDBCCaseResolver;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeArrowTypeConverter;
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
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.COPY_INTO_QUERY_TEMPLATE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.COUNT_RECORDS_QUERY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.DESCRIBE_STORAGE_INTEGRATION_TEMPLATE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.DOUBLE_QUOTE_CHAR;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.JDBC_PROPERTIES;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.LIST_PAGINATED_TABLES_QUERY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.MAX_PARTITION_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.S3_ENHANCED_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SHOW_PRIMARY_KEYS_QUERY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SINGLE_SPLIT_LIMIT_COUNT;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_SPLIT_QUERY_ID;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.STORAGE_INTEGRATION_BUCKET_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.STORAGE_INTEGRATION_CONFIG_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.STORAGE_INTEGRATION_PROPERTY_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.STORAGE_INTEGRATION_PROPERTY_VALUE_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.STORAGE_INTEGRATION_STORAGE_PROVIDER_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.VIEW_CHECK_QUERY;

/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns` in
 * information_schema.
 */
public class SnowflakeMetadataHandler extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);

    private static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String COUNTS_COLUMN_NAME = "COUNTS";
    private static final String PRIMARY_KEY_COLUMN_NAME = "column_name";
    private static final String EMPTY_STRING = StringUtils.EMPTY;
    private static final String ALL_PARTITIONS = "*";

    private S3Client amazonS3;
    private SnowflakeQueryStringBuilder snowflakeQueryStringBuilder = new SnowflakeQueryStringBuilder(DOUBLE_QUOTE_CHAR, new SnowflakeFederationExpressionParser(DOUBLE_QUOTE_CHAR));
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
    public SnowflakeMetadataHandler(
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
        if (request.getConstraints().isQueryPassThrough() && !SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            return;
        }

        // enhance partition schema information for s3 export.
        // during get splits, there is no columns which could result copy into predicate faield.
        // we will need full list of project column here
        if (partitionSchemaBuilder.getField(S3_ENHANCED_PARTITION_COLUMN_NAME) == null && SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            LOGGER.info("enhancePartitionSchema for S3 export {}: Catalog {}, table {}", request.getQueryId(), request.getTableName().getSchemaName(), request.getTableName());
            partitionSchemaBuilder.addField(S3_ENHANCED_PARTITION_COLUMN_NAME, Types.MinorType.VARBINARY.getType());
        }
        else if (partitionSchemaBuilder.getField(BLOCK_PARTITION_COLUMN_NAME) == null) {
            partitionSchemaBuilder.addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        }
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
        TableName tableName = request.getTableName();
        String queryID = request.getQueryId();

        this.handleSnowflakePartitions(request, blockWriter, tableName, queryID);
    }

    @Override
    protected Optional<ArrowType> convertDatasourceTypeToArrow(int columnIndex, int precision, Map<String, String> configOptions, ResultSetMetaData metadata) throws SQLException
    {
        int scale = metadata.getScale(columnIndex);
        int columnType = metadata.getColumnType(columnIndex);

        return SnowflakeArrowTypeConverter.toArrowType(
                columnType,
                precision,
                scale,
                configOptions);
    }

    private void handleSnowflakePartitions(GetTableLayoutRequest request, BlockWriter blockWriter, TableName tableName, String queryID) throws Exception
    {
        LOGGER.debug("getPartitions: {}: Schema {}, table {}", queryID, tableName.getSchemaName(),
                tableName.getTableName());

        // if we are using export method, we don't need to calculate partition
        if (SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            blockWriter.writeRows((Block block, int rowNum) -> {
                block.setValue(S3_ENHANCED_PARTITION_COLUMN_NAME,
                        rowNum,
                        request.getSchema().serializeAsMessage());
                return 1;
            });
            return;
        }

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(getRequestOverrideConfig(request)))) {
            /**
             * "MAX_PARTITION_COUNT" is currently set to 50 to limit the number of partitions.
             * this is to handle timeout issues because of huge partitions
             */
            LOGGER.info(" Total Partition Limit" + MAX_PARTITION_COUNT);
            boolean viewFlag = checkForView(tableName, getRequestOverrideConfig(request));
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
                    Optional<String> primaryKey = getPrimaryKey(tableName, getRequestOverrideConfig(request));
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

    private String buildQueryPassthroughSql(Constraints constraints)
    {
        jdbcQueryPassthrough.verify(constraints.getQueryPassthroughArguments());
        return constraints.getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        if (SnowflakeConstants.isS3ExportEnabled(configOptions)) {
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
        String queryId = request.getQueryId();
        // Sanitize and validate integration name to follow Snowflake naming rules
        Set<Split> splits = new HashSet<>();
        Optional<S3Uri> s3Uri = Optional.empty();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(getRequestOverrideConfig(request)))) {
            String sfIntegrationName = this.getStorageIntegrationName();
            String sfS3ExportPathPrefix = this.getStorageIntegrationS3PathFromSnowFlake(connection, sfIntegrationName);
            String snowflakeExportSQL = this.getSnowFlakeCopyIntoBaseSQL(request);

            // Build S3 path and COPY INTO query
            String s3Path = String.format("%s/%s/%s/", sfS3ExportPathPrefix, queryId, UUID.randomUUID().toString());
            String snowflakeExportQuery = String.format(COPY_INTO_QUERY_TEMPLATE,
                    s3Path, snowflakeExportSQL, snowflakeQueryStringBuilder.quote(sfIntegrationName));
            LOGGER.info("Snowflake Copy Statement: {} for queryId: {}", snowflakeExportQuery, queryId);

            // Get the SQL statement which was created in getPartitions
            LOGGER.debug("doGetSplits: qQryId: {},  Catalog {}, table {}, s3ExportBucketPath:{}, snowflakeExportQuery:{}", queryId,
                    request.getTableName().getSchemaName(),
                    request.getTableName().getTableName(),
                    sfS3ExportPathPrefix,
                    snowflakeExportQuery);
            URI uri = URI.create(s3Path);
            s3Uri = Optional.ofNullable(amazonS3.utilities().parseUri(uri));
            connection.prepareStatement(snowflakeExportQuery).execute();
        }
        catch (SnowflakeSQLException snowflakeSQLException) {
            // handle race condition on another splits already start the copy into statement
            if (!snowflakeSQLException.getMessage().contains("Files already existing")) {
                throw new AthenaConnectorException("Exception in execution export statement " + snowflakeSQLException.getMessage(), snowflakeSQLException,
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
            }
        }
        catch (Exception e) {
            throw new AthenaConnectorException("Exception in execution export statement :" + e.getMessage(), e,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }

        if (s3Uri.isEmpty()) {
            throw new AthenaConnectorException("S3 URI should not be empty for Snowflake S3 Export",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        List<S3Object> s3ObjectSummaries = getlistExportedObjects(s3Uri.get().bucket().orElseThrow(), s3Uri.get().key().orElseThrow());
        LOGGER.debug("{} s3ObjectSummaries returned after executing on SnowFlake for queryId {}",
                (long) s3ObjectSummaries.size(), queryId);

        if (!s3ObjectSummaries.isEmpty()) {
            LOGGER.debug("{} s3ObjectSummaries returned after executing on SnowFlake for queryId {}",
                    (long) s3ObjectSummaries.size(), queryId);
            for (S3Object objectSummary : s3ObjectSummaries) {
                Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add(SNOWFLAKE_SPLIT_QUERY_ID, queryId)
                        .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, s3Uri.get().bucket().orElseThrow())
                        .add(SNOWFLAKE_SPLIT_OBJECT_KEY, objectSummary.key())
                        .build();
                splits.add(split);
            }
            return new GetSplitsResponse(request.getCatalogName(), splits);
        }
        else {
            // Case when there is no data for copy into.
            LOGGER.debug("s3ObjectSummaries returned empty on SnowFlake for queryId {}", queryId);
            Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add(SNOWFLAKE_SPLIT_QUERY_ID, queryId)
                    .add(SNOWFLAKE_SPLIT_EXPORT_BUCKET, s3Uri.get().bucket().orElseThrow())
                    .add(SNOWFLAKE_SPLIT_OBJECT_KEY, EMPTY_STRING)
                    .build();
            splits.add(split);
            return new GetSplitsResponse(request.getCatalogName(), split);
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
    @VisibleForTesting
    List<S3Object> getlistExportedObjects(String s3ExportBucketName, String prefix)
    {
        ListObjectsResponse listObjectsResponse;
        try {
            listObjectsResponse = amazonS3.listObjects(ListObjectsRequest.builder()
                    .bucket(s3ExportBucketName)
                    .prefix(prefix)
                    .build());
        }
        catch (SdkClientException | S3Exception e) {
            String errorMsg = String.format("Failed to list objects in bucket %s with prefix %s", s3ExportBucketName, prefix);
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
    protected Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema, AwsRequestOverrideConfiguration requestOverrideConfiguration)
            throws Exception
    {
        LOGGER.debug("getSchema start, tableName:" + tableName);
        /**
         * query to fetch column data type to handle appropriate datatype to arrowtype conversions.
         */
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData())) {
            // snowflake JDBC doesn't support last() to check number or rows, and getColumns won't raise exception when table not found.
            // need to safeguard when table not found.
            boolean found = false;
            while (resultSet.next()) {
                found = true;
                Optional<ArrowType> columnType = SnowflakeArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);

                String columnName = resultSet.getString(COLUMN_NAME);
                /**
                 * converting into VARCHAR for not supported data types.
                 */
                if (columnType.isEmpty()) {
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }
                else if (!SupportedTypes.isSupported(columnType.get())) {
                    LOGGER.warn("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                    columnType = Optional.of(Types.MinorType.VARCHAR.getType());
                }

                LOGGER.debug(" AddField Schema Building... name:{}, type:{} ", columnName, columnType.get());
                schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType.get()).build());
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
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        // if we are using export, we don't care about partition for table schema
        if (SnowflakeConstants.isS3ExportEnabled(configOptions)) {
            LOGGER.debug("Skipping partition, s3 export enable: " + catalogName);
            return schemaBuilder.build();
        }

        LOGGER.debug("getPartitionSchema: " + catalogName);
        schemaBuilder.addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    private Optional<String> getPrimaryKey(TableName tableName, AwsRequestOverrideConfiguration overrideConfig) throws Exception
    {
        LOGGER.debug("getPrimaryKey tableName: " + tableName);
        List<String> primaryKeys = new ArrayList<String>();
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(overrideConfig))) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(SHOW_PRIMARY_KEYS_QUERY + "\"" + tableName.getSchemaName() + "\".\"" + tableName.getTableName() + "\"");
                 ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    // Concatenate multiple primary keys if they exist
                    primaryKeys.add(rs.getString(PRIMARY_KEY_COLUMN_NAME));
                }
            }

            String primaryKeyString = primaryKeys.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","));
            if (!Strings.isNullOrEmpty(primaryKeyString) && hasUniquePrimaryKey(tableName, primaryKeyString, overrideConfig)) {
                return Optional.of(primaryKeyString);
            }
        }
        return Optional.empty();
    }

    /**
     * Snowflake does not enforce primary key constraints, so we double-check user has unique primary key
     * before partitioning.
     */
    private boolean hasUniquePrimaryKey(TableName tableName, String primaryKey, AwsRequestOverrideConfiguration overrideConfig) throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(overrideConfig))) {
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
    private boolean checkForView(TableName tableName, AwsRequestOverrideConfiguration overrideConfig) throws Exception
    {
        boolean viewFlag = false;
        List<String> viewparameters = Arrays.asList(tableName.getSchemaName(), tableName.getTableName());
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider(overrideConfig))) {
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

    public Optional<String> getSFStorageIntegrationNameFromConfig()
    {
        return Optional.ofNullable(configOptions.get(STORAGE_INTEGRATION_CONFIG_KEY));
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

    @VisibleForTesting
    Optional<Map<String, String>> getStorageIntegrationProperties(Connection connection, String integrationName) throws SQLException
    {
        String checkIntegrationQuery = String.format(DESCRIBE_STORAGE_INTEGRATION_TEMPLATE, integrationName.toUpperCase());
        Map<String, String> storageIntegrationRow = new HashMap<>();
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(checkIntegrationQuery)) {
            while (resultSet.next()) {
                storageIntegrationRow.put(resultSet.getString(STORAGE_INTEGRATION_PROPERTY_KEY),
                        resultSet.getString(STORAGE_INTEGRATION_PROPERTY_VALUE_KEY));
            }
        }
        catch (SQLException e) {
            LOGGER.error("Error checking for integration {}: exception:{}, message: {}", integrationName, e.getClass().getSimpleName(), e.getMessage());
            if (e.getMessage().contains("does not exist or not authorized")) {
                return Optional.empty();
            }
            throw e;
        }
        return Optional.ofNullable(storageIntegrationRow);
    }

    private void validateSFStorageIntegrationExistAndValid(Map<String, String> storageIntegrationMap) throws SQLException
    {
        String s3ExportPath = Optional.ofNullable(storageIntegrationMap.get(STORAGE_INTEGRATION_BUCKET_KEY))
                .orElseThrow(() -> new IllegalArgumentException(String.format("Snowflake Storage Integration, field:%s cannot be null", STORAGE_INTEGRATION_BUCKET_KEY)));

        String provider = Optional.ofNullable(storageIntegrationMap.get(STORAGE_INTEGRATION_STORAGE_PROVIDER_KEY))
                .orElseThrow(() -> new IllegalArgumentException(String.format("Snowflake Storage Integration, field:%s cannot be null", STORAGE_INTEGRATION_STORAGE_PROVIDER_KEY)));

        if (!"S3".equalsIgnoreCase(provider)) {
            throw new IllegalArgumentException(String.format("Snowflake Storage Integration, field:%s must be S3", STORAGE_INTEGRATION_STORAGE_PROVIDER_KEY));
        }

        // Validate it's an S3 path
        if (!s3ExportPath.startsWith("s3://")) {
            throw new IllegalArgumentException(String.format("Storage integration bucket path must be an S3 path: %s", s3ExportPath));
        }

        if (s3ExportPath.split(", ").length != 1) {
            throw new IllegalArgumentException(String.format("Snowflake Storage Integration, field:%s must be a single S3 path", STORAGE_INTEGRATION_BUCKET_KEY));
        }
    }

    @VisibleForTesting
    String getStorageIntegrationS3PathFromSnowFlake(Connection connection, String integrationName) throws SQLException
    {
        Optional<Map<String, String>> storageIntegrationProperties = this.getStorageIntegrationProperties(connection, integrationName);
        if (storageIntegrationProperties.isEmpty()) {
            throw new IllegalArgumentException(String.format("Snowflake Storage Integration: name:%s not found", integrationName));
        }

        validateSFStorageIntegrationExistAndValid(storageIntegrationProperties.get());
        String bucketPath = storageIntegrationProperties.get().get(STORAGE_INTEGRATION_BUCKET_KEY);
        // Normalize trailing slash
        if (bucketPath.endsWith("/")) {
            bucketPath = bucketPath.substring(0, bucketPath.length() - 1);
        }

        return bucketPath;
    }

    /**
     * Get Snowflake storage integration name from config
     * @return
     */
    private String getStorageIntegrationName()
    {
        // Check if integration name is provided in the config.
        return this.getSFStorageIntegrationNameFromConfig().orElseThrow(() -> {
            return new AthenaConnectorException("Snowflake storage integration name not found",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        });
    }

    private String getSnowFlakeCopyIntoBaseSQL(GetSplitsRequest request) throws SQLException
    {
        String generatedSql;
        if (request.getConstraints().isQueryPassThrough()) {
            generatedSql = this.buildQueryPassthroughSql(request.getConstraints());
        }
        else {
            // Get split has no column info, we will need to use the custom partition column we get from GetTableLayOurResponse to obtain information.
            FieldReader fieldReaderPreparedStmt = request.getPartitions().getFieldReader(S3_ENHANCED_PARTITION_COLUMN_NAME);
            ByteBuffer buffer = ByteBuffer.wrap(fieldReaderPreparedStmt.readByteArray());
            Schema schema = Schema.deserializeMessage(buffer);

            generatedSql = snowflakeQueryStringBuilder.getBaseExportSQLString(request.getCatalogName(), request.getTableName().getSchemaName(), request.getTableName().getTableName(),
                    schema,
                    request.getConstraints());
        }
        return generatedSql;
    }

    @Override
    public CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return new SnowflakeCredentialsProvider(secretName, requestOverrideConfiguration);
    }
}
