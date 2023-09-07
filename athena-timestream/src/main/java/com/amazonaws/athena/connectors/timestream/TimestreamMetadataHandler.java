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
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.util.PaginatedRequestIterator;
import com.amazonaws.athena.connectors.timestream.query.QueryFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesRequest;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesResult;
import com.amazonaws.services.timestreamwrite.model.ListTablesResult;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(METADATA_FLAG);

    private static final long MAX_RESULTS = 100_000;

    //Used to generate TimeStream queries using templates query patterns.
    private final QueryFactory queryFactory = new QueryFactory();

    private final AWSGlue glue;
    private final AmazonTimestreamQuery tsQuery;
    private final AmazonTimestreamWrite tsMeta;

    public TimestreamMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        glue = getAwsGlue();
        tsQuery = TimestreamClientBuilder.buildQueryClient(SOURCE_TYPE);
        tsMeta = TimestreamClientBuilder.buildWriteClient(SOURCE_TYPE);
    }

    @VisibleForTesting
    protected TimestreamMetadataHandler(
        AmazonTimestreamQuery tsQuery,
        AmazonTimestreamWrite tsMeta,
        AWSGlue glue,
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glue = glue;
        this.tsQuery = tsQuery;
        this.tsMeta = tsMeta;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        List<String> schemas = PaginatedRequestIterator.stream(this::doListSchemaNamesOnePage, ListDatabasesResult::getNextToken)
            .flatMap(result -> result.getDatabases().stream())
            .map(db -> db.getDatabaseName())
            .collect(Collectors.toList());

        return new ListSchemasResponse(
            request.getCatalogName(),
            schemas);
    }

    private ListDatabasesResult doListSchemaNamesOnePage(String nextToken)
    {
        return tsMeta.listDatabases(new ListDatabasesRequest().withNextToken(nextToken));
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        // First try with the schema name passed in
        try {
            return doListTablesInternal(blockAllocator, request);
        }
        catch (com.amazonaws.services.timestreamwrite.model.ResourceNotFoundException ex) {
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
        ListTablesResult timestreamResults = doListTablesOnePage(request.getSchemaName(), request.getNextToken());
        List<TableName> tableNames = timestreamResults.getTables()
            .stream()
            .map(table -> new TableName(request.getSchemaName(), table.getTableName()))
            .collect(Collectors.toList());

        // Pass through whatever token we got from Glue to the user
        ListTablesResponse result = new ListTablesResponse(
            request.getCatalogName(),
            tableNames,
            timestreamResults.getNextToken());
        logger.debug("doListTables [paginated] result: {}", result);
        return result;
    }

    private ListTablesResult doListTablesOnePage(String schemaName, String nextToken)
    {
        // TODO: We should pass through the pageSize as withMaxResults(pageSize)
        com.amazonaws.services.timestreamwrite.model.ListTablesRequest listTablesRequest =
                new com.amazonaws.services.timestreamwrite.model.ListTablesRequest()
                        .withDatabaseName(schemaName)
                        .withNextToken(nextToken);
        return tsMeta.listTables(listTablesRequest);
    }

    private Stream<TableName> getTableNamesInSchema(String schemaName)
    {
        return PaginatedRequestIterator.stream((pageToken) -> doListTablesOnePage(schemaName, pageToken), ListTablesResult::getNextToken)
            .flatMap(currResult -> currResult.getTables().stream())
            .map(table -> new TableName(schemaName, table.getTableName()));
    }

    private String findSchemaNameIgnoringCase(String schemaNameInsensitive)
    {
        return PaginatedRequestIterator.stream(this::doListSchemaNamesOnePage, ListDatabasesResult::getNextToken)
            .flatMap(result -> result.getDatabases().stream())
            .map(db -> db.getDatabaseName())
            .filter(name -> name.equalsIgnoreCase(schemaNameInsensitive))
            .findAny()
            .orElseThrow(() -> new RuntimeException(String.format("Could not find a case-insensitive match for schema name %s", schemaNameInsensitive)));
    }

    private TableName findTableNameIgnoringCase(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        String caseInsenstiveSchemaNameMatch = findSchemaNameIgnoringCase(getTableRequest.getTableName().getSchemaName());

        // based on AmazonMskMetadataHandler::findGlueRegistryNameIgnoringCasing
        return PaginatedRequestIterator.stream((pageToken) -> doListTablesOnePage(caseInsenstiveSchemaNameMatch, pageToken), ListTablesResult::getNextToken)
            .flatMap(result -> result.getTables().stream())
            .map(tbl -> new TableName(caseInsenstiveSchemaNameMatch, tbl.getTableName()))
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

        QueryRequest queryRequest = new QueryRequest().withQueryString(describeQuery);
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        do {
            QueryResult queryResult = tsQuery.query(queryRequest);
            for (Row next : queryResult.getRows()) {
                List<Datum> datum = next.getData();

                if (datum.size() != 3) {
                    throw new RuntimeException("Unexpected datum size " + datum.size() +
                            " while getting schema from datum[" + datum.toString() + "]");
                }

                Field nextField = TimestreamSchemaUtils.makeField(datum.get(0).getScalarValue(), datum.get(1).getScalarValue());
                schemaBuilder.addField(nextField);
            }
            queryRequest = new QueryRequest().withNextToken(queryResult.getNextToken());
        }
        while (queryRequest.getNextToken() != null);

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
        catch (com.amazonaws.services.timestreamquery.model.ValidationException ex) {
            logger.debug("Could not find table name matching {} in database {}. Falling back to case-insensitive lookup.", request.getTableName().getTableName(), request.getTableName().getSchemaName());

            TableName resolvedTableName = findTableNameIgnoringCase(blockAllocator, request);
            logger.debug("Found case insensitive match for schema {} and table {}", resolvedTableName.getSchemaName(), resolvedTableName.getTableName());
            Schema schema = inferSchemaForTable(resolvedTableName);
            return new GetTableResponse(request.getCatalogName(), resolvedTableName, schema);
        }
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
        Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey()).build();
        return new GetSplitsResponse(request.getCatalogName(), split);
    }
}
