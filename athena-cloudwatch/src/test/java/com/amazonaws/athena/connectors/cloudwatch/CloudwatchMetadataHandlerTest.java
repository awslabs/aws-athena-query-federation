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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private CloudwatchMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private CloudWatchLogsClient mockAwsLogs;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        Mockito.lenient().when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return DescribeLogStreamsResponse.builder()
                    .logStreams(
                            LogStream.builder().logStreamName("table-9").build(),
                            LogStream.builder().logStreamName("table-10").build())
                    .build();
        });

        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return DescribeLogGroupsResponse.builder()
                    .logGroups(
                            LogGroup.builder().logGroupName("schema-1").build(),
                            LogGroup.builder().logGroupName("schema-20").build())
                    .build();
        });
        handler = new CloudwatchMetadataHandler(mockAwsLogs, new LocalKeyFactory(), mockSecretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
            throws TimeoutException
    {
        logger.info("doListSchemas - enter");

        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogGroupsRequest request = (DescribeLogGroupsRequest) invocationOnMock.getArguments()[0];

            DescribeLogGroupsResponse.Builder responseBuilder = DescribeLogGroupsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogGroup> logGroups = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogGroup nextLogGroup = LogGroup.builder().logGroupName("schema-" + String.valueOf(i)).build();
                    logGroups.add(nextLogGroup);
                }
            }

            responseBuilder.logGroups(logGroups);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());

        assertTrue(res.getSchemas().size() == 30);
        verify(mockAwsLogs, times(4)).describeLogGroups(nullable(DescribeLogGroupsRequest.class));
        verifyNoMoreInteractions(mockAwsLogs);

        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables()
            throws TimeoutException
    {
        logger.info("doListTables - enter");

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = LogStream.builder().logStreamName("table-" + String.valueOf(i)).build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default",
                "schema-1", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertTrue(res.getTables().contains(new TableName("schema-1", "all_log_streams")));

        assertTrue(res.getTables().size() == 31);

        verify(mockAwsLogs, times(4)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));
        verify(mockAwsLogs, times(1)).describeLogGroups(nullable(DescribeLogGroupsRequest.class));
        verifyNoMoreInteractions(mockAwsLogs);

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = "schema-20";

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            assertTrue(request.logGroupName().equals(expectedSchema));
            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = LogStream.builder().logStreamName("table-" + String.valueOf(i)).build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        GetTableRequest req = new GetTableRequest(identity, "queryId", "default", new TableName(expectedSchema, "table-9"), Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(new TableName(expectedSchema, "table-9"), res.getTableName());
        assertTrue(res.getSchema() != null);

        verify(mockAwsLogs, times(1)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            DescribeLogStreamsRequest request = (DescribeLogStreamsRequest) invocationOnMock.getArguments()[0];

            DescribeLogStreamsResponse.Builder responseBuilder = DescribeLogStreamsResponse.builder();

            Integer nextToken;
            if (request.nextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.nextToken()) < 3) {
                nextToken = Integer.valueOf(request.nextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                int continuation = request.nextToken() == null ? 0 : Integer.valueOf(request.nextToken());
                for (int i = 0 + continuation * 100; i < 300; i++) {
                    LogStream nextLogStream = LogStream.builder()
                            .logStreamName("table-" + String.valueOf(i))
                            .storedBytes(i * 1000L)
                            .build();
                    logStreams.add(nextLogStream);
                }
            }

            responseBuilder.logStreams(logStreams);
            if (nextToken != null) {
                responseBuilder.nextToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        constraintsMap.put("log_stream",
                EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false)
                        .add("table-10").build());

        Schema schema = SchemaBuilder.newBuilder().addStringField("log_stream").build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName("schema-1", "all_log_streams"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.singleton("log_stream"));

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(res.getPartitions().getSchema().findField("log_stream") != null);
        assertTrue(res.getPartitions().getRowCount() == 1);

        verify(mockAwsLogs, times(4)).describeLogStreams(nullable(DescribeLogStreamsRequest.class));

        logger.info("doGetTableLayout - exit");
    }

    @Test
    public void doGetSplits()
    {
        logger.info("doGetSplits: enter");

        Schema schema = SchemaBuilder.newBuilder()
                .addField(CloudwatchMetadataHandler.LOG_STREAM_FIELD, new ArrowType.Utf8())
                .addField(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD, new ArrowType.Int(64, true))
                .addField(CloudwatchMetadataHandler.LOG_GROUP_FIELD, new ArrowType.Utf8())
                .build();

        Block partitions = allocator.createBlock(schema);

        int num_partitions = 2_000;
        for (int i = 0; i < num_partitions; i++) {
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD), i, 2016L + i);
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_STREAM_FIELD), i, "log_stream_" + i);
            BlockUtils.setValue(partitions.getFieldVector(CloudwatchMetadataHandler.LOG_GROUP_FIELD), i, "log_group_" + i);
        }
        partitions.setRowCount(num_partitions);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName("schema", "all_log_streams"),
                partitions,
                Collections.singletonList(CloudwatchMetadataHandler.LOG_STREAM_FIELD),
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                continuationToken);
        int numContinuations = 0;
        do {
            GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);
            logger.info("doGetSplits: req[{}]", req);

            MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
            assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

            GetSplitsResponse response = (GetSplitsResponse) rawResponse;
            continuationToken = response.getContinuationToken();

            logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

            for (Split nextSplit : response.getSplits()) {
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_STREAM_SIZE_FIELD));
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_STREAM_FIELD));
                assertNotNull(nextSplit.getProperty(CloudwatchMetadataHandler.LOG_GROUP_FIELD));
            }

            if (continuationToken != null) {
                numContinuations++;
            }
        }
        while (continuationToken != null);

        assertTrue(numContinuations > 0);

        logger.info("doGetSplits: exit");
    }
}
