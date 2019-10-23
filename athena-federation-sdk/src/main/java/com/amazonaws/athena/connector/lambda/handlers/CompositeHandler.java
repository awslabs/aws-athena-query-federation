package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
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
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.arrow.util.VisibleForTesting;

public class CompositeHandler
        extends UnifiedHandler
{
    private final MetadataHandler metadataHandler;
    private final RecordHandler recordHandler;

    public CompositeHandler(MetadataHandler metadataHandler, RecordHandler recordHandler, String sourceType)
    {
        this(AmazonS3ClientBuilder.standard().build(), metadataHandler, recordHandler, sourceType);
    }

    @VisibleForTesting
    protected CompositeHandler(AmazonS3 amazonS3, MetadataHandler metadataHandler, RecordHandler recordHandler, String sourceType)
    {
        super(amazonS3, sourceType);
        this.metadataHandler = metadataHandler;
        this.recordHandler = recordHandler;
    }

    @Override
    protected void readWithConstraint(ConstraintEvaluator constraintEvaluator, BlockSpiller spiller, ReadRecordsRequest recordsRequest)
            throws Exception
    {
        recordHandler.readWithConstraint(constraintEvaluator, spiller, recordsRequest);
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            throws Exception
    {
        return metadataHandler.doListSchemaNames(allocator, request);
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            throws Exception
    {
        return metadataHandler.doListTables(allocator, request);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
            throws Exception
    {
        return metadataHandler.doGetTable(allocator, request);
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator allocator, GetTableLayoutRequest request)
            throws Exception
    {
        return metadataHandler.doGetTableLayout(allocator, request);
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            throws Exception
    {
        return metadataHandler.doGetSplits(allocator, request);
    }
}
