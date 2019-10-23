package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DocDBMetadataHandler
        extends GlueMetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandler.class);

    private static final String SOURCE_TYPE = "documentdb";
    private static final String GLUE_ENV_VAR = "disable_glue";
    private static final int SCHEMA_INFERRENCE_NUM_DOCS = 4;

    private final AWSGlue glue;
    private final Map<String, MongoClient> clientCache;

    public DocDBMetadataHandler()
    {
        super((System.getenv(GLUE_ENV_VAR) == null) ? AWSGlueClientBuilder.standard().build() : null, SOURCE_TYPE);
        glue = getAwsGlue();
        clientCache = new HashMap<>();
    }

    @VisibleForTesting
    protected DocDBMetadataHandler(AWSGlue glue,
            Map<String, MongoClient> clientCache,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(glue, keyFactory, secretsManager, SOURCE_TYPE, spillBucket, spillPrefix);
        this.glue = glue;
        this.clientCache = clientCache;
    }

    private MongoClient getOrCreateClient(MetadataRequest request)
    {
        String catalog = request.getCatalogName();
        MongoClient result = clientCache.get(catalog);
        if (result == null) {
            result = MongoClients.create(System.getenv(catalog));
            clientCache.put(catalog, result);
        }
        return result;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request)
    {
        List<String> schemas = new ArrayList<>();
        MongoClient client = getOrCreateClient(request);
        try (MongoCursor<String> itr = client.listDatabaseNames().iterator()) {

            while (itr.hasNext()) {
                schemas.add(itr.next());
            }

            return new ListSchemasResponse(request.getCatalogName(), schemas);
        }
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request)
    {
        MongoClient client = getOrCreateClient(request);
        List<TableName> tables = new ArrayList<>();

        try (MongoCursor<String> itr = client.getDatabase(request.getSchemaName()).listCollectionNames().iterator()) {
            while (itr.hasNext()) {
                tables.add(new TableName(request.getSchemaName(), itr.next()));
            }

            return new ListTablesResponse(request.getCatalogName(), tables);
        }
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request)
    {
        try {
            if (glue != null) {
                return super.doGetTable(blockAllocator, request);
            }
        }
        catch (Exception ex) {
            logger.warn("doGetTable: Unable to retrieve table[{}] from AWSGlue.", request.getTableName(), ex);
        }

        MongoClient client = getOrCreateClient(request);
        TableName tableName = request.getTableName();
        Schema schema = SchemaUtils.inferSchema(client, tableName, SCHEMA_INFERRENCE_NUM_DOCS);

        return new GetTableResponse(request.getCatalogName(), tableName, schema);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
        //Even though our table doesn't support complex layouts or partitioning, we need to convey that there is at least
        //1 partition to read as part of the query or Athena will assume partition pruning found no candidate layouts to read.
        Block partitions = BlockUtils.newBlock(blockAllocator, "partitionId", Types.MinorType.INT.getType(), 0);
        return new GetTableLayoutResponse(request.getCatalogName(), request.getTableName(), partitions, new HashSet<>());
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
    {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        //Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey()).build());
    }

    @Override
    protected Field convertField(String name, String glueType)
    {
        return GlueFieldLexer.lex(name, glueType);
    }
}
