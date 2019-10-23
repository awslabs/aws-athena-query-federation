package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
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
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CloudwatchMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetadataHandler.class);

    private static final String sourceType = "cloudwatch";
    //some customers have a very large number of log groups and log streams. In those cases we limit
    //the max results as a safety mechanism.
    private static final long MAX_RESULTS = 10_000;
    protected static final int MAX_SPLITS_PER_REQUEST = 1000;

    private static final String ALL_LOG_STREAMS_TABLE = "all_log_streams";
    protected static final String LOG_STREAM_FIELD = "log_stream";
    protected static final String LOG_TIME_FIELD = "time";
    protected static final String LOG_MSG_FIELD = "message";
    protected static final String LOG_STREAM_SIZE_FIELD = "log_stream_bytes";
    protected static final Schema CLOUDWATCH_SCHEMA;

    static {
        CLOUDWATCH_SCHEMA = new SchemaBuilder().newBuilder()
                .addField(LOG_STREAM_FIELD, Types.MinorType.VARCHAR.getType())
                .addField(LOG_TIME_FIELD, new ArrowType.Int(64, true))
                .addField(LOG_MSG_FIELD, Types.MinorType.VARCHAR.getType())
                //requests to read multiple log streams can be parallelized so lets treat it like a partition
                .addMetadata("partitionCols", LOG_STREAM_FIELD)
                .build();
    }

    private final AWSLogs awsLogs;

    public CloudwatchMetadataHandler()
    {
        super(sourceType);
        awsLogs = AWSLogsClientBuilder.standard().build();
    }

    @VisibleForTesting
    protected CloudwatchMetadataHandler(AWSLogs awsLogs,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            String spillBucket,
            String spillPrefix)
    {
        super(keyFactory, secretsManager, sourceType, spillBucket, spillPrefix);
        this.awsLogs = awsLogs;
    }

    @Override
    protected ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
    {
        DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();
        DescribeLogGroupsResult result;
        List<String> schemas = new ArrayList<>();
        do {
            if (schemas.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log groups, exceeded max metadata results for schema count.");
            }
            result = awsLogs.describeLogGroups(request);
            result.getLogGroups().forEach(next -> schemas.add(next.getLogGroupName()));
            request.setNextToken(result.getNextToken());
            logger.info("doListSchemaNames: Listing log groups {} {}", result.getNextToken(), schemas.size());
        }
        while (result.getNextToken() != null);

        return new ListSchemasResponse(listSchemasRequest.getCatalogName(), schemas);
    }

    @Override
    protected ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
    {
        DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(listTablesRequest.getSchemaName());
        DescribeLogStreamsResult result;
        List<TableName> tables = new ArrayList<>();
        do {
            if (tables.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log streams, exceeded max metadata results for table count.");
            }
            result = awsLogs.describeLogStreams(request);
            result.getLogStreams().forEach(next -> tables.add(new TableName(listTablesRequest.getSchemaName(), next.getLogStreamName())));
            request.setNextToken(result.getNextToken());
            logger.info("doListTables: Listing log streams {} {}", result.getNextToken(), tables.size());
        }
        while (result.getNextToken() != null);

        //We add a special table that represents all log streams. This is helpful depending on how
        //you have your logs organized.
        tables.add(new TableName(listTablesRequest.getSchemaName(), ALL_LOG_STREAMS_TABLE));

        return new ListTablesResponse(listTablesRequest.getCatalogName(), tables);
    }

    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        TableName tableName = getTableRequest.getTableName();
        validateTable(tableName);
        return new GetTableResponse(getTableRequest.getCatalogName(), getTableRequest.getTableName(), CLOUDWATCH_SCHEMA);
    }

    private void validateTable(TableName tableName)
    {
        if (ALL_LOG_STREAMS_TABLE.equals(tableName.getTableName())) {
            DescribeLogGroupsRequest validationRequest = new DescribeLogGroupsRequest()
                    .withLogGroupNamePrefix(tableName.getSchemaName());
            DescribeLogGroupsResult validationResult = awsLogs.describeLogGroups(validationRequest);
            for (LogGroup next : validationResult.getLogGroups()) {
                if (next.getLogGroupName().equals(tableName.getSchemaName())) {
                    return;
                }
            }

            throw new RuntimeException("Unknown table name " + tableName);
        }
        else {
            DescribeLogStreamsRequest validationRequest = new DescribeLogStreamsRequest()
                    .withLogGroupName(tableName.getSchemaName())
                    .withLogStreamNamePrefix(tableName.getTableName());
            DescribeLogStreamsResult validationResult = awsLogs.describeLogStreams(validationRequest);
            for (LogStream next : validationResult.getLogStreams()) {
                if (next.getLogStreamName().equals(tableName.getTableName())) {
                    return;
                }
            }
            throw new RuntimeException("Unknown table name " + tableName);
        }
    }

    @Override
    protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest getTableLayoutRequest)
    {
        TableName tableName = getTableLayoutRequest.getTableName();

        Schema partitionsSchema = SchemaBuilder.newBuilder()
                .addField(LOG_STREAM_FIELD, Types.MinorType.VARCHAR.getType())
                .addField(LOG_STREAM_SIZE_FIELD, new ArrowType.Int(64, true))
                .addMetadata("partitionCols", LOG_STREAM_FIELD)
                .build();

        DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(tableName.getSchemaName());
        if (!ALL_LOG_STREAMS_TABLE.equals(tableName.getTableName())) {
            request.setLogStreamNamePrefix(tableName.getTableName());
        }

        DescribeLogStreamsResult result;
        int partitionCount = 0;

        Block partitions = blockAllocator.createBlock(partitionsSchema);
        try (ConstraintEvaluator constraintEvaluator = new ConstraintEvaluator(blockAllocator,
                partitions.getSchema(),
                getTableLayoutRequest.getConstraints())) {
            do {
                result = awsLogs.describeLogStreams(request);
                for (LogStream next : result.getLogStreams()) {
                    //Each log stream that matches any possible partition pruning should be added to the partition list.
                    if (constraintEvaluator.apply(LOG_STREAM_FIELD, next.getLogStreamName())) {
                        BlockUtils.setValue(partitions.getFieldVector(LOG_STREAM_FIELD), partitionCount, next.getLogStreamName());
                        BlockUtils.setValue(partitions.getFieldVector(LOG_STREAM_SIZE_FIELD), partitionCount, next.getStoredBytes());
                        partitionCount++;
                    }
                }
                request.setNextToken(result.getNextToken());
            }
            while (result.getNextToken() != null);

            partitions.setRowCount(partitionCount);
        }
        catch (Exception ex) {
            logger.error("doGetTableLayout: Error", ex);
            throw new RuntimeException(ex);
        }

        logger.info("doGetTableLayout: Found {} partitions.", partitionCount);

        return new GetTableLayoutResponse(getTableLayoutRequest.getCatalogName(),
                getTableLayoutRequest.getTableName(),
                partitions,
                Collections.singleton(LOG_STREAM_FIELD));
    }

    @Override
    protected GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {

        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {

            FieldReader locationReader = partitions.getFieldReader(CloudwatchMetadataHandler.LOG_STREAM_FIELD);
            locationReader.setPosition(curPartition);

            FieldReader sizeReader = partitions.getFieldReader(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD);
            locationReader.setPosition(curPartition);

            //Every split must have a unique location if we wish to spill to avoid failures
            SpillLocation spillLocation = makeSpillLocation(request);

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(CloudwatchMetadataHandler.LOG_STREAM_FIELD, String.valueOf(locationReader.readText()))
                    .add(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD, String.valueOf(sizeReader.readLong()));

            splits.add(splitBuilder.build());

            if (splits.size() >= MAX_SPLITS_PER_REQUEST) {
                //We exceeded the number of split we want to return in a single request, return and provide
                //a continuation token.
                return new GetSplitsResponse(request.getCatalogName(),
                        splits,
                        encodeContinuationToken(curPartition));
            }
        }

        return new GetSplitsResponse(request.getCatalogName(), splits, null);
    }

    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
