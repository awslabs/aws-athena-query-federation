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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.DescribeLogStreamsRequest;
import com.amazonaws.services.logs.model.DescribeLogStreamsResult;
import com.amazonaws.services.logs.model.LogGroup;
import com.amazonaws.services.logs.model.LogStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
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

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private CloudwatchMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private AWSLogs mockAwsLogs;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        Mockito.lenient().when(mockAwsLogs.describeLogStreams(nullable(DescribeLogStreamsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return new DescribeLogStreamsResult().withLogStreams(new LogStream().withLogStreamName("table-9"),
                    new LogStream().withLogStreamName("table-10"));
        });

        when(mockAwsLogs.describeLogGroups(nullable(DescribeLogGroupsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            return new DescribeLogGroupsResult().withLogGroups(new LogGroup().withLogGroupName("schema-1"),
                    new LogGroup().withLogGroupName("schema-20"));
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

            DescribeLogGroupsResult result = new DescribeLogGroupsResult();

            Integer nextToken;
            if (request.getNextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.getNextToken()) < 3) {
                nextToken = Integer.valueOf(request.getNextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogGroup> logGroups = new ArrayList<>();
            if (request.getNextToken() == null || Integer.valueOf(request.getNextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogGroup nextLogGroup = new LogGroup();
                    nextLogGroup.setLogGroupName("schema-" + String.valueOf(i));
                    logGroups.add(nextLogGroup);
                }
            }

            result.withLogGroups(logGroups);
            if (nextToken != null) {
                result.setNextToken(String.valueOf(nextToken));
            }

            return result;
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

            DescribeLogStreamsResult result = new DescribeLogStreamsResult();

            Integer nextToken;
            if (request.getNextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.getNextToken()) < 3) {
                nextToken = Integer.valueOf(request.getNextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.getNextToken() == null || Integer.valueOf(request.getNextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = new LogStream();
                    nextLogStream.setLogStreamName("table-" + String.valueOf(i));
                    logStreams.add(nextLogStream);
                }
            }

            result.withLogStreams(logStreams);
            if (nextToken != null) {
                result.setNextToken(String.valueOf(nextToken));
            }

            return result;
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

            assertTrue(request.getLogGroupName().equals(expectedSchema));
            DescribeLogStreamsResult result = new DescribeLogStreamsResult();

            Integer nextToken;
            if (request.getNextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.getNextToken()) < 3) {
                nextToken = Integer.valueOf(request.getNextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.getNextToken() == null || Integer.valueOf(request.getNextToken()) < 3) {
                for (int i = 0; i < 10; i++) {
                    LogStream nextLogStream = new LogStream();
                    nextLogStream.setLogStreamName("table-" + String.valueOf(i));
                    logStreams.add(nextLogStream);
                }
            }

            result.withLogStreams(logStreams);
            if (nextToken != null) {
                result.setNextToken(String.valueOf(nextToken));
            }

            return result;
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

            DescribeLogStreamsResult result = new DescribeLogStreamsResult();

            Integer nextToken;
            if (request.getNextToken() == null) {
                nextToken = 1;
            }
            else if (Integer.valueOf(request.getNextToken()) < 3) {
                nextToken = Integer.valueOf(request.getNextToken()) + 1;
            }
            else {
                nextToken = null;
            }

            List<LogStream> logStreams = new ArrayList<>();
            if (request.getNextToken() == null || Integer.valueOf(request.getNextToken()) < 3) {
                int continuation = request.getNextToken() == null ? 0 : Integer.valueOf(request.getNextToken());
                for (int i = 0 + continuation * 100; i < 300; i++) {
                    LogStream nextLogStream = new LogStream();
                    nextLogStream.setLogStreamName("table-" + String.valueOf(i));
                    nextLogStream.setStoredBytes(i * 1000L);
                    logStreams.add(nextLogStream);
                }
            }

            result.withLogStreams(logStreams);
            if (nextToken != null) {
                result.setNextToken(String.valueOf(nextToken));
            }

            return result;
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
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
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
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
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
