/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
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
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
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
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchExceptionFilter.EXCEPTION_FILTER;

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
    private static final long MAX_RESULTS = 100_000;
    //The maximum number of splits that will be generated by a single call to doGetSplits(...) before we paginate.
    protected static final int MAX_SPLITS_PER_REQUEST = 1000;
    //The name of the special table view which allows you to query all log streams in a LogGroup
    protected static final String ALL_LOG_STREAMS_TABLE = "all_log_streams";
    //The name of the log stream field in our response and split objects.
    protected static final String LOG_STREAM_FIELD = "log_stream";
    //The name of the log group field in our response and split objects.
    protected static final String LOG_GROUP_FIELD = "log_group";
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
    private final ThrottlingInvoker invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER).build();
    private final CloudwatchTableResolver tableResolver;

    public CloudwatchMetadataHandler()
    {
        super(sourceType);
        this.awsLogs = AWSLogsClientBuilder.standard().build();
        tableResolver = new CloudwatchTableResolver(invoker, awsLogs, MAX_RESULTS, MAX_RESULTS);
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
        tableResolver = new CloudwatchTableResolver(invoker, awsLogs, MAX_RESULTS, MAX_RESULTS);
    }

    /**
     * List LogGroups in your Cloudwatch account treating each as a 'schema' (aka database)
     *
     * @see MetadataHandler
     */
    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest listSchemasRequest)
            throws TimeoutException
    {
        DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();
        DescribeLogGroupsResult result;
        List<String> schemas = new ArrayList<>();
        do {
            if (schemas.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log groups, exceeded max metadata results for schema count.");
            }
            result = invoker.invoke(() -> awsLogs.describeLogGroups(request));
            result.getLogGroups().forEach(next -> schemas.add(next.getLogGroupName().toLowerCase()));
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
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest listTablesRequest)
            throws TimeoutException
    {
        String logGroupName = tableResolver.validateSchema(listTablesRequest.getSchemaName());
        DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(logGroupName);
        DescribeLogStreamsResult result;
        List<TableName> tables = new ArrayList<>();
        do {
            if (tables.size() > MAX_RESULTS) {
                throw new RuntimeException("Too many log streams, exceeded max metadata results for table count.");
            }
            result = invoker.invoke(() -> awsLogs.describeLogStreams(request));
            result.getLogStreams().forEach(next -> tables.add(toTableName(listTablesRequest, next)));
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
    public GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest getTableRequest)
    {
        TableName tableName = getTableRequest.getTableName();
        tableResolver.validateTable(tableName);
        return new GetTableResponse(getTableRequest.getCatalogName(),
                getTableRequest.getTableName(),
                CLOUDWATCH_SCHEMA,
                Collections.singleton(LOG_STREAM_FIELD));
    }

    /**
     * We add one additional field to the partition schema. This field is used for our own purposes and ignored
     * by Athena but it will get passed to calls to GetSplits(...) which is where we will set it on our Split
     * without the need to call Cloudwatch a second time.
     *
     * @see MetadataHandler
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        partitionSchemaBuilder.addField(LOG_STREAM_SIZE_FIELD, new ArrowType.Int(64, true));
        partitionSchemaBuilder.addField(LOG_GROUP_FIELD, Types.MinorType.VARCHAR.getType());
    }

    /**
     * Gets the list of LogStreams that need to be scanned to satisfy the requested table. In most cases this will be just
     * 1 LogStream and this results in just 1 partition. If, however, the request is for the special ALL_LOG_STREAMS view
     * then all LogStreams in the requested LogGroup (schema) are queried and turned into partitions 1:1.
     *
     * @note This method applies partition pruning based on the log_stream field.
     * @see MetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request)
            throws Exception
    {
        CloudwatchTableName cwTableName = tableResolver.validateTable(request.getTableName());

        DescribeLogStreamsRequest cwRequest = new DescribeLogStreamsRequest(cwTableName.getLogGroupName());
        if (!ALL_LOG_STREAMS_TABLE.equals(cwTableName.getLogStreamName())) {
            cwRequest.setLogStreamNamePrefix(cwTableName.getLogStreamName());
        }

        DescribeLogStreamsResult result;
        do {
            result = invoker.invoke(() -> awsLogs.describeLogStreams(cwRequest));
            for (LogStream next : result.getLogStreams()) {
                //Each log stream that matches any possible partition pruning should be added to the partition list.
                blockWriter.writeRows((Block block, int rowNum) -> {
                    boolean matched = block.setValue(LOG_GROUP_FIELD, rowNum, cwRequest.getLogGroupName());
                    matched &= block.setValue(LOG_STREAM_FIELD, rowNum, next.getLogStreamName());
                    matched &= block.setValue(LOG_STREAM_SIZE_FIELD, rowNum, next.getStoredBytes());
                    return matched ? 1 : 0;
                });
            }
            cwRequest.setNextToken(result.getNextToken());
        }
        while (result.getNextToken() != null);
    }

    /**
     * Each partition is converted into a single Split which means we will potentially read all LogStreams required for
     * the query in parallel.
     *
     * @see MetadataHandler
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        int partitionContd = decodeContinuationToken(request);
        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        for (int curPartition = partitionContd; curPartition < partitions.getRowCount(); curPartition++) {
            FieldReader logStreamReader = partitions.getFieldReader(LOG_STREAM_FIELD);
            logStreamReader.setPosition(curPartition);

            FieldReader logGroupReader = partitions.getFieldReader(LOG_GROUP_FIELD);
            logGroupReader.setPosition(curPartition);

            FieldReader sizeReader = partitions.getFieldReader(LOG_STREAM_SIZE_FIELD);
            sizeReader.setPosition(curPartition);

            //Every split must have a unique location if we wish to spill to avoid failures
            SpillLocation spillLocation = makeSpillLocation(request);

            Split.Builder splitBuilder = Split.newBuilder(spillLocation, makeEncryptionKey())
                    .add(CloudwatchMetadataHandler.LOG_GROUP_FIELD, String.valueOf(logGroupReader.readText()))
                    .add(CloudwatchMetadataHandler.LOG_STREAM_FIELD, String.valueOf(logStreamReader.readText()))
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

    /**
     * Helper that converts a LogStream to a TableName by lowercasing the schema of the request and the logstreamname.
     *
     * @param request The ListTablesRequest to retrieve the schema name from.
     * @param logStream The LogStream to turn into a table.
     * @return A TableName with both the schema (LogGroup) and the table (LogStream) lowercased.
     */
    private TableName toTableName(ListTablesRequest request, LogStream logStream)
    {
        return new TableName(request.getSchemaName(), logStream.getLogStreamName());
    }
}
