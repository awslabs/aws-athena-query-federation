/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.util.PaginatedRequestIterator;
import com.amazonaws.athena.connectors.timestream.qpt.TimestreamQueryPassthrough;
import com.amazonaws.athena.connectors.timestream.query.QueryFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ColumnInfo;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Database;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

public class TimestreamMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamMetadataHandler.class);

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "timestream";
    //The Glue table property that indicates that a table matching the name of an TimeStream table
    //is indeed enabled for use by this connector.
    private static final String METADATA_FLAG = "timestream-metadata-flag";
    //Used to filter out Glue tables which lack a timestream metadata flag.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.parameters().containsKey(METADATA_FLAG);

    private static final long MAX_RESULTS = 100_000;

    //Used to generate TimeStream queries using templates query patterns.
    private final QueryFactory queryFactory = new QueryFactory();

    private final GlueClient glue;
    private final TimestreamQueryClient tsQuery;
    private final TimestreamWriteClient tsMeta;

    private final TimestreamQueryPassthrough queryPassthrough;

    public TimestreamMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        glue = getAwsGlue();
        tsQuery = TimestreamClientBuilder.buildQueryClient(SOURCE_TYPE);
        tsMeta = TimestreamClientBuilder.buildWriteClient(SOURCE_TYPE);
        queryPassthrough = new TimestreamQueryPassthrough();
    }

    @VisibleForTesting
    protected TimestreamMetadataHandler(
        TimestreamQueryClient tsQuery,
        TimestreamWriteClient tsMeta,
        GlueClient glue,
        EncryptionKeyFactory keyFactory,
        SecretsManagerClient secretsManager,
        AthenaClient athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glue = glue;
        this.tsQuery = tsQuery;
        this.tsMeta = tsMeta;
        queryPassthrough = new TimestreamQueryPassthrough();
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        this.queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, this.configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        List<String> schemas = PaginatedRequestIterator.stream(this::doListSchemaNamesOnePage, ListDatabasesResponse::nextToken)
            .flatMap(result -> result.databases().stream())
            .map(Database::databaseName)
            .collect(Collectors.toList());

        return new ListSchemasResponse(
            request.getCatalogName(),
            schemas);
    }

    private ListDatabasesResponse doListSchemaNamesOnePage(String nextToken)
    {
        return tsMeta.listDatabases(ListDatabasesRequest.builder().nextToken(nextToken).build());
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        // First try with the schema name passed in
        try {
            return doListTablesInternal(blockAllocator, request);
        }
        catch (software.amazon.awssdk.services.timestreamwrite.model.ResourceNotFoundException ex) {
            // If it fails then we will retry after resolving the schema name by ignoring the casing
            String resolvedSchemaName = findSchemaNameIgnoringCase(request.getSchemaName());
            request = new ListTablesRequest(request.getIdentity(), request.getQueryId(), request.getCatalogName(), resolvedSchemaName, request.getNextToken(), request.getPageSize());
            return doListTablesInternal(blockAllocator, request);
        }
    }

    private ListTablesResponse doListTablesInternal(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        logger.info("doListTablesInternal: {}", request);
        // In this situation we want to loop through all the pages to return up to the MAX_RESULTS size
        // And only do this if we don't have a token passed in, otherwise if we have a token that takes precedence
        // over the fact that the page size was set to unlimited.
        if (request.getPageSize() == UNLIMITED_PAGE_SIZE_VALUE && request.getNextToken() == null) {
            logger.info("Request page size is UNLIMITED_PAGE_SIZE_VALUE");

            List<TableName> allTableNames = getTableNamesInSchema(request.getSchemaName())
                .limit(MAX_RESULTS + 1)
                .collect(Collectors.toList());

            if (allTableNames.size() > MAX_RESULTS) {
                throw new RuntimeException(
                    String.format("Exceeded maximum result size. Current doListTables result size: %d", allTableNames.size()));
            }
            ListTablesResponse result = new ListTablesResponse(request.getCatalogName(), allTableNames, null);
            logger.debug("doListTables result: {}", result);
            return result;
        }

        // Otherwise don't retrieve all pages, just pass through the page token.
        software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse timestreamResults = doListTablesOnePage(request.getSchemaName(), request.getNextToken());
        List<TableName> tableNames = timestreamResults.tables()
            .stream()
            .map(table -> new TableName(request.getSchemaName(), table.tableName()))
            .collect(Collectors.toList());

        // Pass through whatever token we got from Glue to the user
        ListTablesResponse result = new ListTablesResponse(
            request.getCatalogName(),
            tableNames,
            timestreamResults.nextToken());
        logger.debug("doListTables [paginated] result: {}", result);
        return result;
    }

    private software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse doListTablesOnePage(String schemaName, String nextToken)
    {
        // TODO: We should pass through the pageSize as withMaxResults(pageSize)
        software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest listTablesRequest = software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest.builder()
                .databaseName(schemaName)
                .nextToken(nextToken)
                .build();
        return tsMeta.listTables(listTablesRequest);
    }

    private Stream<TableName> getTableNamesInSchema(String schemaName)
    {
        return PaginatedRequestIterator.stream((pageToken) -> doListTablesOnePage(schemaName, pageToken), software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse::nextToken)
            .flatMap(currResult -> currResult.tables().stream())
            .map(table -> new TableName(schemaName, table.tableName()));
    }

    private String findSchemaNameIgnoringCase(String schemaNameInsensitive)
    {
        return PaginatedRequestIterator.stream(this::doListSchemaNamesOnePage, ListDatabasesResponse::nextToken)
            .flatMap(result -> result.databases().stream())
            .map(Database::databaseName)
            .filter(name -> name.equalsIgnoreCase(schemaNameInsensitive))
            .findAny()
            .orElseThrow(() -> new RuntimeException(String.format("Could not find a case-insensitive match for schema name %s", schemaNameInsensitive)));
    }

    private TableName findTableNameIgnoringCase(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        String caseInsenstiveSchemaNameMatch = findSchemaNameIgnoringCase(getTableRequest.getTableName().getSchemaName());

        // based on AmazonMskMetadataHandler::findGlueRegistryNameIgnoringCasing
        return PaginatedRequestIterator.stream((pageToken) -> doListTablesOnePage(caseInsenstiveSchemaNameMatch, pageToken), software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse::nextToken)
            .flatMap(result -> result.tables().stream())
            .map(tbl -> new TableName(caseInsenstiveSchemaNameMatch, tbl.tableName()))
            .filter(tbl -> tbl.getTableName().equalsIgnoreCase(getTableRequest.getTableName().getTableName()))
            .findAny()
            .orElseThrow(() -> new RuntimeException(String.format("Could not find a case-insensitive match for table name %s", getTableRequest.getTableName().getTableName())));
    }

   private Schema inferSchemaForTable(TableName tableName)
   {
        String describeQuery = queryFactory.createDescribeTableQueryBuilder()
                .withTablename(tableName.getTableName())
                .withDatabaseName(tableName.getSchemaName())
                .build();

        logger.info("doGetTable: Retrieving schema for table[{}] from TimeStream using describeQuery[{}].",
                tableName, describeQuery);

       QueryRequest queryRequest = QueryRequest.builder().queryString(describeQuery).build();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        do {
            QueryResponse queryResult = tsQuery.query(queryRequest);
            for (Row next : queryResult.rows()) {
                List<Datum> datum = next.data();

                if (datum.size() != 3) {
                    throw new RuntimeException("Unexpected datum size " + datum.size() +
                            " while getting schema from datum[" + datum.toString() + "]");
                }

                Field nextField = TimestreamSchemaUtils.makeField(datum.get(0).scalarValue(), datum.get(1).scalarValue());
                schemaBuilder.addField(nextField);
            }
            queryRequest = QueryRequest.builder().nextToken(queryResult.nextToken()).build();
        }
        while (queryRequest.nextToken() != null);

        return schemaBuilder.build();
   } 

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: enter", request.getTableName());

        if (glue != null) {
            try {
                return super.doGetTable(blockAllocator, request, TABLE_FILTER);
            }
            catch (RuntimeException ex) {
                logger.warn("doGetTable: Unable to retrieve table[{}:{}] from AWS Glue.",
                    request.getTableName().getSchemaName(),
                    request.getTableName().getTableName(),
                    ex);
            }
        }

        try {
            Schema schema = inferSchemaForTable(request.getTableName());
            return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
        }
        catch (software.amazon.awssdk.services.timestreamquery.model.ValidationException ex) {
            logger.debug("Could not find table name matching {} in database {}. Falling back to case-insensitive lookup.", request.getTableName().getTableName(), request.getTableName().getSchemaName());

            TableName resolvedTableName = findTableNameIgnoringCase(blockAllocator, request);
            logger.debug("Found case insensitive match for schema {} and table {}", resolvedTableName.getSchemaName(), resolvedTableName.getTableName());
            Schema schema = inferSchemaForTable(resolvedTableName);
            return new GetTableResponse(request.getCatalogName(), resolvedTableName, schema);
        }
    }

    @Override
    public GetTableResponse doGetQueryPassthroughSchema(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        if (!request.isQueryPassthrough()) {
            throw new IllegalArgumentException("No Query passed through [{}]" + request);
        }

        queryPassthrough.verify(request.getQueryPassthroughArguments());
        String customerPassedQuery = request.getQueryPassthroughArguments().get(TimestreamQueryPassthrough.QUERY);
        QueryRequest queryRequest = QueryRequest.builder().queryString(customerPassedQuery).maxRows(1).build();
        // Timestream Query does not provide a way to conduct a dry run or retrieve metadata results without execution. Therefore, we need to "seek" at least once before obtaining metadata.
        QueryResponse queryResult = tsQuery.query(queryRequest);
        List<ColumnInfo> columnInfo = queryResult.columnInfo();
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        for (ColumnInfo column : columnInfo) {
            Field nextField = TimestreamSchemaUtils.makeField(column.name(), column.type().scalarTypeAsString().toLowerCase());
            schemaBuilder.addField(nextField);
        }

        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schemaBuilder.build(), Collections.emptySet());
    }

    /**
     * Our table doesn't support complex layouts or partitioning so we simply make this method a NoOp.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker
            queryStatusChecker)
            throws Exception
    {
        //NoOp as we do not support partitioning.
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws Exception
    {
        //Since we do not support connector level parallelism for this source at the moment, we generate a single
        //basic split.
        Split split;
        if (request.getConstraints().isQueryPassThrough()) {
            logger.info("QPT Split Requested");
            Map<String, String> qptArguments = request.getConstraints().getQueryPassthroughArguments();
            split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).applyProperties(qptArguments).build();
        }
        else {
            split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).build();
        }

        return new GetSplitsResponse(request.getCatalogName(), split);
    }
}
