package com.amazonaws.athena.connectors.aws.cmdb;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;
import java.util.Map;

public class AwsCmdbMetadataHandler
        extends MetadataHandler
{
    private static final String sourceType = "cmdb";
    private Map<String, List<TableName>> schemas;
    private Map<TableName, TableProvider> tableProviders;

    public AwsCmdbMetadataHandler()
    {
        super(sourceType);
        TableProviderFactory tableProviderFactory = new TableProviderFactory();
        schemas = tableProviderFactory.getSchemas();
        tableProviders = tableProviderFactory.getTableProviders();
    }

    @VisibleForTesting
    protected AwsCmdbMetadataHandler(TableProviderFactory tableProviderFactory,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        schemas = tableProviderFactory.getSchemas();
        tableProviders = tableProviderFactory.getTableProviders();
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas.keySet());
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        return new ListTablesResponse(listTablesRequest.getCatalogName(), schemas.get(listTablesRequest.getSchemaName()));
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        TableProvider tableProvider = tableProviders.get(getTableRequest.getTableName());
        return tableProvider.getTable(blockAllocator, getTableRequest);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
    {
        TableProvider tableProvider = tableProviders.get(getTableLayoutRequest.getTableName());
        return tableProvider.getTableLayout(blockAllocator, getTableLayoutRequest);
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest)
    {
        //Every split needs a unique spill location.
        SpillLocation spillLocation = makeSpillLocation(getSplitsRequest);
        EncryptionKey encryptionKey = makeEncryptionKey();
        Split split = Split.newBuilder(spillLocation, encryptionKey).build();
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(), split);
    }
}
