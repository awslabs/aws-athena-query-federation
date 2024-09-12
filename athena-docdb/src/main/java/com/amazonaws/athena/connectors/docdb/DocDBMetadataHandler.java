/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.docdb.qpt.DocDBQueryPassthrough;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

/**
 * Handles metadata requests for the Athena DocumentDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Uses a Glue table property (docfb-metadata-flag) to indicate that the table (whose name matched the DocDB collection
 * name) can indeed be used to supplement metadata from DocDB itself.
 * 2. Attempts to resolve sensitive fields such as DocDB connection strings via SecretsManager so that you can substitute
 * variables with values from by doing something like:
 * mongodb://${docdb_instance_1_creds}@myhostname.com:123/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0
 */
public class DocDBMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandler.class);

    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "documentdb";
    //Field name used to store the connection string as a property on Split objects.
    protected static final String DOCDB_CONN_STR = "connStr";
    //The Env variable name used to store the default DocDB connection string if no catalog specific
    //env variable is set.
    private static final String DEFAULT_DOCDB = "default_docdb";
    //The Glue table property that indicates that a table matching the name of an DocDB table
    //is indeed enabled for use by this connector.
    private static final String DOCDB_METADATA_FLAG = "docdb-metadata-flag";
    //Used to filter out Glue tables which lack a docdb metadata flag.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(DOCDB_METADATA_FLAG);
    //The number of documents to scan when attempting to infer schema from an DocDB collection.
    private static final int SCHEMA_INFERRENCE_NUM_DOCS = 10;
    // used to filter out Glue databases which lack the docdb-metadata-flag in the URI.
    private static final DatabaseFilter DB_FILTER = (Database database) -> (database.getLocationUri() != null && database.getLocationUri().contains(DOCDB_METADATA_FLAG));

    private final AWSGlue glue;
    private final DocDBConnectionFactory connectionFactory;
    private final DocDBQueryPassthrough queryPassthrough = new DocDBQueryPassthrough();

    public DocDBMetadataHandler(java.util.Map<String, String> configOptions)
    {
        super(SOURCE_TYPE, configOptions);
        glue = getAwsGlue();
        connectionFactory = new DocDBConnectionFactory();
    }

    @VisibleForTesting
    protected DocDBMetadataHandler(
        AWSGlue glue,
        DocDBConnectionFactory connectionFactory,
        EncryptionKeyFactory keyFactory,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        String spillBucket,
        String spillPrefix,
        java.util.Map<String, String> configOptions)
    {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.glue = glue;
        this.connectionFactory = connectionFactory;
    }

    private MongoClient getOrCreateConn(MetadataRequest request)
    {
        String endpoint = resolveSecrets(getConnStr(request));
        return connectionFactory.getOrCreateConn(endpoint);
    }

    /**
     * Retrieves the DocDB connection details from an env variable matching the catalog name, if no such
     * env variable exists we fall back to the default env variable defined by DEFAULT_DOCDB.
     */
    private String getConnStr(MetadataRequest request)
    {
        String conStr = configOptions.get(request.getCatalogName());
        if (conStr == null) {
            logger.info("getConnStr: No environment variable found for catalog {} , using default {}",
                    request.getCatalogName(), DEFAULT_DOCDB);
            conStr = configOptions.get(DEFAULT_DOCDB);
        }
        return conStr;
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        queryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);

        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * List databases in your DocumentDB instance treating each as a 'schema' (aka database)
     *
     * @see GlueMetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request) throws Exception
    {
        Set<String> combinedSchemas = new LinkedHashSet<>();
        if (glue != null) {
            try {
                combinedSchemas.addAll(super.doListSchemaNames(blockAllocator, request, DB_FILTER).getSchemas());
            }
            catch (RuntimeException e) {
                logger.warn("doListSchemaNames: Unable to retrieve schemas from AWSGlue.", e);
            }
        }  
    
        List<String> schemas = new ArrayList<>();
        MongoClient client = getOrCreateConn(request);
        try (MongoCursor<String> itr = client.listDatabaseNames().iterator()) {
            while (itr.hasNext()) {
                //On MongoDB, Schema return empties if no permission settings
                String schema = itr.next();
                if (!Strings.isNullOrEmpty(schema)) {
                    schemas.add(schema);
                }
            }
            combinedSchemas.addAll(schemas);
        }
        return new ListSchemasResponse(request.getCatalogName(), combinedSchemas);
    }

    /**
     * List collections in the requested schema in your DocumentDB instance treating the requested schema as an DocumentDB
     * database.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request) throws Exception
    {
        logger.info("Getting tables in {}", request.getSchemaName());
        Set<TableName> combinedTables = new LinkedHashSet<>();
        String token = request.getNextToken();
        if (token == null && glue != null) {
            try {
                combinedTables.addAll(super.doListTables(blockAllocator, new ListTablesRequest(request.getIdentity(), request.getQueryId(), 
                    request.getCatalogName(), request.getSchemaName(), null, UNLIMITED_PAGE_SIZE_VALUE), TABLE_FILTER).getTables());
            }
            catch (RuntimeException e) {
                logger.warn("doListTables: Unable to retrieve tables from AWSGlue in database/schema {}", request.getSchemaName(), e);
            }
        }
    
        MongoClient client = getOrCreateConn(request);
        Stream<String> tableNames = doListTablesWithCommand(client, request);
        int startToken = request.getNextToken() != null ? Integer.parseInt(request.getNextToken()) : 0;
        int pageSize = request.getPageSize();
        String nextToken = null;

        if (pageSize != UNLIMITED_PAGE_SIZE_VALUE) {
            logger.info("Starting at token {} w/ page size {}", startToken, pageSize);
            tableNames = tableNames.skip(startToken).limit(request.getPageSize());
            nextToken = Integer.toString(startToken + pageSize);
        }

        List<TableName> paginatedTables = tableNames.map(tableName -> new TableName(request.getSchemaName(), tableName)).collect(Collectors.toList());
        combinedTables.addAll(paginatedTables);
        logger.info("doListTables returned {} tables. Next token is {}", paginatedTables.size(), nextToken);
        return new ListTablesResponse(request.getCatalogName(), new ArrayList<>(combinedTables), nextToken);
    }

    /**
     * This method uses MongoDB command line call to retrieve only list of collections that owner has permission to.
     *
     * Currently, Mongo Java client does not support additional config/parameters on listCollections for authorizedCollections only.
     * Attempt use Mongo Java client `listCollectionNames` to read whole collections without permission for 1+ collection will result in exception
     *
     * Example return document
     * {
     *   cursor: {
     *     id: Long("0"),
     *     ns: 'sample_analytics.$cmd.listCollections',
     *     firstBatch: [
                ......
     *       { name: 'people', type: 'collection' }
     *     ]
     *   },
     *  .....
     *   }
     * @param client
     * @param request
     * @return
     */
    private Stream<String> doListTablesWithCommand(MongoClient client, ListTablesRequest request)
    {
        logger.debug("doListTablesWithCommand Start");
        Document queryDocument = new Document("listCollections", 1).append("nameOnly", true).append("authorizedCollections", true);
        Document document = client.getDatabase(request.getSchemaName()).runCommand(queryDocument);

        List<Document> list = ((Document) document.get("cursor")).getList("firstBatch", Document.class);
        return list.stream().map(doc -> doc.getString("name")).sorted();
    }

    /**
     * If Glue is enabled as a source of supplemental metadata we look up the requested Schema/Table in Glue and
     * filters out any results that don't have the DOCDB_METADATA_FLAG set. If no matching results were found in Glue,
     * then we resort to inferring the schema of the DocumentDB collection using SchemaUtils.inferSchema(...). If there
     * is no such table in DocumentDB the operation will fail.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
            throws Exception
    {
        logger.info("doGetTable: enter", request.getTableName());
        String schemaNameInput;
        String tableNameInput;

        if (request.isQueryPassthrough()) {
            queryPassthrough.verify(request.getQueryPassthroughArguments());
            schemaNameInput = request.getQueryPassthroughArguments().get(DocDBQueryPassthrough.DATABASE);
            tableNameInput = request.getQueryPassthroughArguments().get(DocDBQueryPassthrough.COLLECTION);
        }
        else {
            schemaNameInput = request.getTableName().getSchemaName();
            tableNameInput = request.getTableName().getTableName();
        }

        TableName tableName = new TableName(schemaNameInput, tableNameInput);
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
            logger.info("doGetTable: Inferring schema for table[{}].", request.getTableName());
            MongoClient client = getOrCreateConn(request);
            //Attempt to update schema and table name with case insensitive match if enable
            schemaNameInput = DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(configOptions, client, schemaNameInput);
            MongoDatabase db = client.getDatabase(schemaNameInput);
            tableNameInput = DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(configOptions, db, tableNameInput);
            tableName = new TableName(schemaNameInput, tableNameInput);
            schema = SchemaUtils.inferSchema(db, tableName, SCHEMA_INFERRENCE_NUM_DOCS);
        }
        return new GetTableResponse(request.getCatalogName(), tableName, schema);
    }

    @Override
    public GetTableResponse doGetQueryPassthroughSchema(BlockAllocator allocator, GetTableRequest request) throws Exception
    {
        return doGetTable(allocator, request);
    }

    /**
     * Our table doesn't support complex layouts or partitioning so we simply make this method a NoOp.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            throws Exception
    {
        //NoOp as we do not support partitioning.
    }

    /**
     * Since our connector does not support parallel scans we generate a single Split and include the connection details
     * as a property on the split so that the RecordHandler has easy access to it.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
    {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        //Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(DOCDB_CONN_STR, getConnStr(request))
                        .build());
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }
}
