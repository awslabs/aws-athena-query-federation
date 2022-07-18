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
import com.amazonaws.services.timestreamwrite.model.Database;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesRequest;
import com.amazonaws.services.timestreamwrite.model.ListDatabasesResult;
import com.amazonaws.services.timestreamwrite.model.ListTablesResult;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimestreamMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamMetadataHandler.class);

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "timestream";
    //The Env variable name used to indicate that we want to disable the use of Glue DataCatalog for supplemental
    //metadata and instead rely solely on the connector's schema inference capabilities.
    private static final String GLUE_ENV = "disable_glue";
    //The Glue table property that indicates that a table matching the name of an TimeStream table
    //is indeed enabled for use by this connector.
    private static final String METADATA_FLAG = "timestream-metadata-flag";
    //Used to filter out Glue tables which lack a timestream metadata flag.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(METADATA_FLAG);
    //Used to generate TimeStream queries using templates query patterns.
    private final QueryFactory queryFactory = new QueryFactory();

    private final AWSGlue glue;
    private final AmazonTimestreamQuery tsQuery;
    private final AmazonTimestreamWrite tsMeta;

    public TimestreamMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        glue = getAwsGlue();
        tsQuery = TimestreamClientBuilder.buildQueryClient(SOURCE_TYPE);
        tsMeta = TimestreamClientBuilder.buildWriteClient(SOURCE_TYPE);
    }

    @VisibleForTesting
    protected TimestreamMetadataHandler(AmazonTimestreamQuery tsQuery,
            AmazonTimestreamWrite tsMeta,
            AWSGlue glue,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
    {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
        this.glue = glue;
        this.tsQuery = tsQuery;
        this.tsMeta = tsMeta;
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        List<String> schemaNames = new ArrayList<>();
        ListDatabasesRequest listDatabasesRequest = new ListDatabasesRequest();
        ListDatabasesResult nextResult = tsMeta.listDatabases(listDatabasesRequest);
        List<Database> nextDatabases = nextResult.getDatabases();
        while (!nextDatabases.isEmpty()) {
            nextDatabases.stream().forEach(next -> schemaNames.add(next.getDatabaseName()));
            if (nextResult.getNextToken() != null && !nextResult.getNextToken().isEmpty()) {
                listDatabasesRequest.setNextToken(nextResult.getNextToken());
                nextResult = tsMeta.listDatabases(listDatabasesRequest);
                nextDatabases = nextResult.getDatabases();
            }
            else {
                nextDatabases = Collections.EMPTY_LIST;
            }
        }

        return new ListSchemasResponse(request.getCatalogName(), schemaNames);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        List<TableName> tableNames = new ArrayList<>();
        com.amazonaws.services.timestreamwrite.model.ListTablesRequest listTablesRequest =
                new com.amazonaws.services.timestreamwrite.model.ListTablesRequest()
                        .withDatabaseName(request.getSchemaName());

        ListTablesResult nextResult = tsMeta.listTables(listTablesRequest);
        List<com.amazonaws.services.timestreamwrite.model.Table> nextTables = nextResult.getTables();
        while (!nextTables.isEmpty()) {
            nextTables.stream().forEach(next -> tableNames.add(new TableName(request.getSchemaName(), next.getTableName())));
            if (nextResult.getNextToken() != null && !nextResult.getNextToken().isEmpty()) {
                listTablesRequest.setNextToken(nextResult.getNextToken());
                nextResult = tsMeta.listTables(listTablesRequest);
                nextTables = nextResult.getTables();
            }
            else {
                nextTables = Collections.EMPTY_LIST;
            }
        }

        return new ListTablesResponse(request.getCatalogName(), tableNames, null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: enter", request.getTableName());

        Schema schema = null;
        try {
            if (glue != null) {
                schema = super.doGetTable(blockAllocator, request, TABLE_FILTER).getSchema();
                logger.info("doGetTable: Retrieved schema for table[{}] from AWS Glue.", request.getTableName());
            }
        }
        catch (RuntimeException ex) {
            logger.warn("doGetTable: Unable to retrieve table[{}:{}] from AWS Glue.",
                    request.getTableName().getSchemaName(),
                    request.getTableName().getTableName(),
                    ex);
        }

        if (schema == null) {
            TableName tableName = request.getTableName();
            String describeQuery = queryFactory.createDescribeTableQueryBuilder()
                    .withTablename(tableName.getTableName())
                    .withDatabaseName(tableName.getSchemaName())
                    .build();

            logger.info("doGetTable: Retrieving schema for table[{}] from TimeStream using describeQuery[{}].",
                    request.getTableName(), describeQuery);

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

            schema = schemaBuilder.build();
        }

        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
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
