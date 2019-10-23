package com.amazonaws.athena.connectors.android;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
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
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.Types;

import java.util.Collections;
import java.util.HashSet;

public class AndroidMetadataHandler
        extends MetadataHandler
{
    private static final String sourceType = "android";
    private static final AndroidDeviceTable androidDeviceTable = new AndroidDeviceTable();

    public AndroidMetadataHandler() {super(sourceType);}

    @VisibleForTesting
    protected AndroidMetadataHandler(
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        String schemaName = androidDeviceTable.getTableName().getSchemaName();
        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), Collections.singletonList(schemaName));
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        return new ListTablesResponse(listTablesRequest.getCatalogName(),
                Collections.singletonList(androidDeviceTable.getTableName()));
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        if (!androidDeviceTable.getTableName().equals(getTableRequest.getTableName())) {
            throw new RuntimeException("Unknown table " + getTableRequest.getTableName());
        }

        return new GetTableResponse(getTableRequest.getCatalogName(),
                androidDeviceTable.getTableName(),
                androidDeviceTable.getSchema());
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
    {
        Block partitions = BlockUtils.newBlock(blockAllocator,
                "partitionId",
                Types.MinorType.INT.getType(),
                0);

        return new GetTableLayoutResponse(request.getCatalogName(),
                request.getTableName(),
                partitions,
                new HashSet<>());
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
