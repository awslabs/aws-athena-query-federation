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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
    //The Env variable name used to indicate that we want to disable the use of Glue DataCatalog for supplemental
    //metadata and instead rely solely on the connector's schema inference capabilities.
    private static final String GLUE_ENV = "disable_glue";
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

    private final AWSGlue glue;
    private final DocDBConnectionFactory connectionFactory;

    public DocDBMetadataHandler()
    {
        //Disable Glue if the env var is present and not explicitly set to "false"
        super((System.getenv(GLUE_ENV) != null && !"false".equalsIgnoreCase(System.getenv(GLUE_ENV))), SOURCE_TYPE);
        glue = getAwsGlue();
        connectionFactory = new DocDBConnectionFactory();
    }

    @VisibleForTesting
    protected DocDBMetadataHandler(AWSGlue glue,
            DocDBConnectionFactory connectionFactory,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix)
    {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix);
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
        String conStr = System.getenv(request.getCatalogName());
        if (conStr == null) {
            logger.info("getConnStr: No environment variable found for catalog {} , using default {}",
                    request.getCatalogName(), DEFAULT_DOCDB);
            conStr = System.getenv(DEFAULT_DOCDB);
        }
        return conStr;
    }

    /**
     * List databases in your DocumentDB instance treating each as a 'schema' (aka database)
     *
     * @see GlueMetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
    {
        List<String> schemas = new ArrayList<>();
        MongoClient client = getOrCreateConn(request);
        try (MongoCursor<String> itr = client.listDatabaseNames().iterator()) {
            while (itr.hasNext()) {
                schemas.add(itr.next());
            }

            return new ListSchemasResponse(request.getCatalogName(), schemas);
        }
    }

    /**
     * List collections in the requested schema in your DocumentDB instance treating the requested schema as an DocumentDB
     * database.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
    {
        MongoClient client = getOrCreateConn(request);
        List<TableName> tables = new ArrayList<>();

        try (MongoCursor<String> itr = client.getDatabase(request.getSchemaName()).listCollectionNames().iterator()) {
            while (itr.hasNext()) {
                tables.add(new TableName(request.getSchemaName(), itr.next()));
            }

            return new ListTablesResponse(request.getCatalogName(), tables);
        }
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
            schema = SchemaUtils.inferSchema(client, request.getTableName(), SCHEMA_INFERRENCE_NUM_DOCS);
        }
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
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
