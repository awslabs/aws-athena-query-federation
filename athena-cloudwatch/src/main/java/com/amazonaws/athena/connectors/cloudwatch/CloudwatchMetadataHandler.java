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

/**
 * Handles metadata requests for the Athena Cloudwatch Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Each LogGroup is treated as a schema (aka database).
 * 2. Each LogStream is treated as a table.
 * 3. A special 'all_log_streams' view is added which allows you to query all LogStreams in a LogGroup.
 * 4. LogStreams area treated as partitions and scanned in parallel.
 * 5. Timestamp predicates are pushed into Cloudwatch itself.
 */
public class CloudwatchMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetadataHandler.class);

    //Used to tag log lines generated by this connector for diagnostic purposes when interacting with Athena.
    private static final String sourceType = "cloudwatch";
    //some customers have a very large number of log groups and log streams. In those cases we limit
    //the max results as a safety mechanism. They can still be queried but aren't returned in show tables or show databases.
    private static final long MAX_RESULTS = 10_000;
    //The maximum number of splits that will be generated by a single call to doGetSplits(...) before we paginate.
    protected static final int MAX_SPLITS_PER_REQUEST = 1000;
    //The name of the special table view which allows you to query all log streams in a LogGroup
    private static final String ALL_LOG_STREAMS_TABLE = "all_log_streams";
    //The name of the log stream field in our response and split objects.
    protected static final String LOG_STREAM_FIELD = "log_stream";
    //The name of the log time field in our response and split objects.
    protected static final String LOG_TIME_FIELD = "time";
    //The name of the log message field in our response and split objects.
    protected static final String LOG_MSG_FIELD = "message";
    //The name of the log stream size field in our split objects.
    protected static final String LOG_STREAM_SIZE_FIELD = "log_stream_bytes";
    //The the schema of all Cloudwatch tables.
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

    /**
     * List LogGroups in your Cloudwatch account treating each as a 'schema' (aka database)
     *
     * @see MetadataHandler
     */
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

    /**
     * List LogStreams within the requested schema (aka LogGroup) in your Cloudwatch account treating each as a 'table'.
     *
     * @see MetadataHandler
     */
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

    /**
     * Returns the pre-set schema for the request Cloudwatch table (LogStream) and schema (LogGroup) after
     * validating that it exists.
     *
     * @see MetadataHandler
     */
    @Override
    protected GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        TableName tableName = getTableRequest.getTableName();
        validateTable(tableName);
        return new GetTableResponse(getTableRequest.getCatalogName(),
                getTableRequest.getTableName(),
                CLOUDWATCH_SCHEMA,
                Collections.singleton(LOG_STREAM_FIELD));
    }

    /**
     * Helper method which validates that the provided TableName exists as a LogGroup / LogStream in Cloudwatch.
     *
     * @param tableName The TableName to validate.
     */
    private void validateTable(TableName tableName)
    {
        //Here we handle the special all_log_streams view by validating only the LogGroup.
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

    /**
     * Gets the list of LogStreams that need to be scanned to satisfy the requested table. In most cases this will be just
     * 1 LogStream and this result in just 1 partition. If, however, the request is for the special ALL_LOG_STREAMS view
     * then all LogStreams in the requested LogGroup (schema) are queried and turned into partitions 1:1.
     *
     * @note This method applies partition pruning based on the log_stream field.
     * @see MetadataHandler
     */
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

    /**
     * Each partition is converted into a single Split which means we will potentially read all LogStreams required for
     * the query in parallel.
     *
     * @see MetadataHandler
     */
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

    /**
     * Used to handle paginated requests.
     *
     * @return The partition number to resume with.
     */
    private int decodeContinuationToken(GetSplitsRequest request)
    {
        if (request.hasContinuationToken()) {
            return Integer.valueOf(request.getContinuationToken());
        }

        //No continuation token present
        return 0;
    }

    /**
     * Used to create pagination tokens by encoding the number of the next partition to process.
     *
     * @param partition The number of the next partition we should process on the next call.
     * @return The encoded continuation token.
     */
    private String encodeContinuationToken(int partition)
    {
        return String.valueOf(partition);
    }
}
