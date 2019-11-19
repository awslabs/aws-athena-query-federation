package com.amazonaws.athena.connector.lambda.handlers;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class allows you to leverage AWS Glue's DataCatalog to satisfy portions of the functionality required in a
 * MetadataHandler. More precisely, this implementation uses AWS Glue's DataCatalog to implement:
 * 1. doListSchemas(...)
 * 2. doListTables(...)
 * 3. doGetTable(...)
 * <p>
 * When you extend this class you can optionally provide a DatabaseFilter and/or TableFilter to decide which Databases
 * (aka schemas) and Tables are eligible for use with your connector. You can find examples of this in the
 * athena-hbase and athena-docdb connector modules. A common reason for this is when you happen to have databases/tables
 * in Glue which match the names of databases and tables in your source but that aren't actually relevant. In such cases
 * you may choose to ignore those Glue tables.
 * <p>
 * At present this class does not retrieve partition information from AWS Glue's DataCatalog. There is an open task
 * for how best to handle partitioning information in this class: https://github.com/awslabs/aws-athena-query-federation/issues/5
 * It is unclear at this time how many sources will have meaningful partition info in Glue but many sources (DocDB, Hbase, Redis)
 * benefited from having basic schema information in Glue. As a result we punted support for partition information to
 * a later time.
 *
 * @note All schema names, table names, and column names must be lower case at this time. Any entities that are uppercase or
 * mixed case will not be accessible in queries and will be lower cased by Athena's engine to ensure consistency across
 * sources. As such you may need to handle this when integrating with a source that supports mixed case. As an example,
 * you can look at the CloudwatchTableResolver in the athena-cloudwatch module for one potential approach to this challenge.
 * @see MetadataHandler
 */
public abstract class GlueMetadataHandler
        extends MetadataHandler
{
    //name of the environment variable that can be used to set which Glue catalog to use (e.g. setting this to
    //a different aws account id allows you to use cross-account catalogs)
    private static final String CATALOG_NAME_ENV_OVERRIDE = "glue_catalog";

    private final AWSGlue awsGlue;

    /**
     * Basic constructor which is recommended when extending this class.
     *
     * @param disable Whether to disable Glue usage. Useful for users that wish to rely on their handlers' schema inference.
     * @param sourceType The source type, used in diagnostic logging.
     */
    public GlueMetadataHandler(boolean disable, String sourceType)
    {
        super(sourceType);
        if (disable) {
            //Only disable if env var is present and not explicitly set to false
            awsGlue = null;
        }
        else {
            awsGlue = AWSGlueClientBuilder.standard()
                    //Override the connection timeout.
                    //The default is 10 seconds, which when retried is 40 seconds.
                    //Lower to 250 ms, 1 second with retry.
                    .withClientConfiguration(new ClientConfiguration().withConnectionTimeout(250))
                    .build();
        }
    }

    /**
     * Constructor that allows injection of a customized Glue client.
     *
     * @param awsGlue The glue client to use.
     * @param sourceType The source type, used in diagnostic logging.
     */
    public GlueMetadataHandler(AWSGlue awsGlue, String sourceType)
    {
        super(sourceType);
        this.awsGlue = awsGlue;
    }

    /**
     * Full DI constructor used mostly for testing
     *
     * @param awsGlue The glue client to use.
     * @param encryptionKeyFactory The EncryptionKeyFactory to use for spill encryption.
     * @param secretsManager The AWSSecretsManager client that can be used when attempting to resolve secrets.
     * @param athena The Athena client that can be used to fetch query termination status to fast-fail this handler.
     * @param spillBucket The S3 Bucket to use when spilling results.
     * @param spillPrefix The S3 prefix to use when spilling results.
     */
    @VisibleForTesting
    protected GlueMetadataHandler(AWSGlue awsGlue,
            EncryptionKeyFactory encryptionKeyFactory,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            String sourceType,
            String spillBucket,
            String spillPrefix)
    {
        super(encryptionKeyFactory, secretsManager, athena, sourceType, spillBucket, spillPrefix);
        this.awsGlue = awsGlue;
    }

    /**
     * Provides access to the Glue client if the extender should need it. This will return null if Glue
     * use is disabled.
     *
     * @return The AWSGlue client being used by this class, or null if disabled.
     */
    protected AWSGlue getAwsGlue()
    {
        return awsGlue;
    }

    /**
     * Provides access to the current AWS Glue DataCatalog being used by this class.
     *
     * @param request The request for which we'd like to resolve the catalog.
     * @return The glue catalog to use for the request.
     */
    protected String getCatalog(MetadataRequest request)
    {
        String override = System.getenv(CATALOG_NAME_ENV_OVERRIDE);
        if (override == null) {
            return request.getIdentity().getAccount();
        }
        return override;
    }

    /**
     * Returns an unfiltered list of schemas (aka databases) from AWS Glue DataCatalog.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return The ListSchemasResponse which mostly contains the list of schemas (aka databases).
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
            throws Exception
    {
        return doListSchemaNames(blockAllocator, request, null);
    }

    /**
     * Returns a list of schemas (aka databases) from AWS Glue DataCatalog with optional filtering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @param filter The DatabaseFilter to apply to all schemas (aka databases) before adding them to the results list.
     * @return The ListSchemasResponse which mostly contains the list of schemas (aka databases).
     */
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request, DatabaseFilter filter)
            throws Exception
    {
        GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest();
        getDatabasesRequest.setCatalogId(getCatalog(request));

        List<String> schemas = new ArrayList<>();
        String nextToken = null;
        do {
            getDatabasesRequest.setNextToken(nextToken);
            GetDatabasesResult result = awsGlue.getDatabases(getDatabasesRequest);

            for (Database next : result.getDatabaseList()) {
                if (filter == null || filter.filter(next)) {
                    schemas.add(next.getName());
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    /**
     * Returns an unfiltered list of tables from AWS Glue DataCatalog for the requested schema (aka database)
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @return The ListTablesResponse which mostly contains the list of table names.
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
            throws Exception
    {
        return doListTables(blockAllocator, request, null);
    }

    /**
     * Returns a list of tables from AWS Glue DataCatalog with optional filtering for the requested schema (aka database)
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog they are querying.
     * @param filter The TableFilter to apply to all tables before adding them to the results list.
     * @return The ListTablesResponse which mostly contains the list of table names.
     */
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request, TableFilter filter)
            throws Exception
    {
        GetTablesRequest getTablesRequest = new GetTablesRequest();
        getTablesRequest.setCatalogId(getCatalog(request));
        getTablesRequest.setDatabaseName(request.getSchemaName());

        Set<TableName> tables = new HashSet<>();
        String nextToken = null;
        do {
            getTablesRequest.setNextToken(nextToken);
            GetTablesResult result = awsGlue.getTables(getTablesRequest);

            for (Table next : result.getTableList()) {
                if (filter == null || filter.filter(next)) {
                    tables.add(new TableName(request.getSchemaName(), next.getName()));
                }
            }

            nextToken = result.getNextToken();
        }
        while (nextToken != null);

        return new ListTablesResponse(request.getCatalogName(), tables);
    }

    /**
     * Attempts to retrieve a Table (columns and properties) from AWS Glue for the request schema (aka database) and table
     * name with no fitlering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @return A GetTableResponse mostly containing the columns, their types, and any table properties for the requested table.
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        return doGetTable(blockAllocator, request, null);
    }

    /**
     * Attempts to retrieve a Table (columns and properties) from AWS Glue for the request schema (aka database) and table
     * name with no filtering.
     *
     * @param blockAllocator Tool for creating and managing Apache Arrow Blocks.
     * @param request Provides details on who made the request and which Athena catalog, database, and table they are querying.
     * @param filter The TableFilter to apply to any matching table before generating the result.
     * @return A GetTableResponse mostly containing the columns, their types, and any table properties for the requested table.
     * @note This method throws a RuntimeException if not table matching the requested criteria (and filter) is found.
     */
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request, TableFilter filter)
            throws Exception
    {
        TableName tableName = request.getTableName();
        com.amazonaws.services.glue.model.GetTableRequest getTableRequest = new com.amazonaws.services.glue.model.GetTableRequest();
        getTableRequest.setCatalogId(getCatalog(request));
        getTableRequest.setDatabaseName(tableName.getSchemaName());
        getTableRequest.setName(tableName.getTableName());

        GetTableResult result = awsGlue.getTable(getTableRequest);
        Table table = result.getTable();

        if (filter != null && !filter.filter(table)) {
            throw new RuntimeException("No matching table found " + request.getTableName());
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        table.getParameters().entrySet().forEach(next -> schemaBuilder.addMetadata(next.getKey(), next.getValue()));

        Set<String> partitionCols = table.getPartitionKeys()
                .stream().map(next -> next.getName()).collect(Collectors.toSet());

        for (Column next : table.getStorageDescriptor().getColumns()) {
            schemaBuilder.addField(convertField(next.getName(), next.getType()));
            if (next.getComment() != null) {
                schemaBuilder.addMetadata(next.getName(), next.getComment());
            }
        }

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                schemaBuilder.build(),
                partitionCols);
    }

    /**
     * Maps a Glue field to an Apache Arrow Field.
     *
     * @param name The name of the field in Glue.
     * @param glueType The type of the field in Glue.
     * @return The corresponding Apache Arrow Field.
     * @note You can override this implementation to provide your own mappings.
     */
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }

    public interface TableFilter
    {
        /**
         * Used to filter table results.
         *
         * @param table The table to evaluate.
         * @return True if the provided table should be in the result, False if not.
         */
        boolean filter(Table table);
    }

    public interface DatabaseFilter
    {
        /**
         * Used to filter database results.
         *
         * @param database The database to evaluate.
         * @return True if the provided database should be in the result, False if not.
         */
        boolean filter(Database database);
    }
}
