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
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsResponse;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.ArgumentMatchers.argThat;
import com.amazonaws.athena.connectors.cloudwatch.qpt.CloudwatchQueryPassthrough;
import software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryResponse;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchRecordHandlerTest.class);
    private static final Long BLOCK_SIZE = 100_000_000L;
    private static final int LOG_EVENTS_PAGE_SIZE = 100_000;

    // Managed-connector (Athena federation) Substrait plans, used by the tests below. These are real
    // plans produced by the same Isthmus SqlToSubstrait + SchemaSerDe path the DNA test runner uses,
    // over the Cloudwatch table schema [log_stream VARCHAR, time BIGINT, message VARCHAR]. They are
    // embedded as constants because the connector does not depend on Isthmus/Calcite. Regenerate from
    // the SQL noted on each constant if the schema or Substrait version changes.
    // SELECT * FROM "cloudwatch-test-stream" WHERE "time" > 1700000000000
    private static final String PLAN_TIME_GT =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAEQARoKZ3Q6YW55X2FueRrZARLWAQq4ATq1AQoHEgUKAwMEBRKHARKEAQoCCgASVwpVCgIKABIxCgpsb2dfc3RyZWFtCgR0aW1lCgdtZXNzYWdlEhQKBGICEAEKBDoCEAEKBGICEAEYAjocCgJkYgoWY2xvdWR3YXRjaC10ZXN0LXN0cmVhbRolGiMIARoECgIQASIMGgoSCAoEEgIIASIAIgsaCQoHOIDQlf+8MRoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgASCmxvZ19zdHJlYW0SBHRpbWUSB21lc3NhZ2UyCxBKKgdpc3RobXVz";
    // SELECT * FROM "cloudwatch-test-stream" WHERE "time" >= 1700000000000 AND "time" <= 1700000100000
    private static final String PLAN_TIME_BETWEEN =
            "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIQGg4IARABGghhbmQ6Ym9vbBITGhEIAhACGgtndGU6YW55X2FueRITGhEIAhADGgtsdGU6YW55X2FueRqQAhKNAgrvATrsAQoHEgUKAwMEBRK+ARK7AQoCCgASVwpVCgIKABIxCgpsb2dfc3RyZWFtCgR0aW1lCgdtZXNzYWdlEhQKBGICEAEKBDoCEAEKBGICEAEYAjocCgJkYgoWY2xvdWR3YXRjaC10ZXN0LXN0cmVhbRpcGloIARoECgIQASInGiUaIwgCGgQKAhABIgwaChIICgQSAggBIgAiCxoJCgc4gNCV/7wxIicaJRojCAMaBAoCEAEiDBoKEggKBBICCAEiACILGgkKBzig3Zv/vDEaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAEgpsb2dfc3RyZWFtEgR0aW1lEgdtZXNzYWdlMgsQSioHaXN0aG11cw==";
    // SELECT * FROM "cloudwatch-test-stream" WHERE "time" = 1700000000000
    private static final String PLAN_TIME_EQUAL =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrZARLWAQq4ATq1AQoHEgUKAwMEBRKHARKEAQoCCgASVwpVCgIKABIxCgpsb2dfc3RyZWFtCgR0aW1lCgdtZXNzYWdlEhQKBGICEAEKBDoCEAEKBGICEAEYAjocCgJkYgoWY2xvdWR3YXRjaC10ZXN0LXN0cmVhbRolGiMIARoECgIQASIMGgoSCAoEEgIIASIAIgsaCQoHOIDQlf+8MRoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgASCmxvZ19zdHJlYW0SBHRpbWUSB21lc3NhZ2UyCxBKKgdpc3RobXVz";
    // SELECT * FROM "cloudwatch-test-stream" WHERE "message" = 'err'  (non-time predicate; not pushable)
    private static final String PLAN_MESSAGE_EQUAL =
            "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFRoTCAEQARoNZXF1YWw6YW55X2FueRrXARLUAQq2ATqzAQoHEgUKAwMEBRKFARKCAQoCCgASVwpVCgIKABIxCgpsb2dfc3RyZWFtCgR0aW1lCgdtZXNzYWdlEhQKBGICEAEKBDoCEAEKBGICEAEYAjocCgJkYgoWY2xvdWR3YXRjaC10ZXN0LXN0cmVhbRojGiEIARoECgIQASIMGgoSCAoEEgIIAiIAIgkaBwoFYgNlcnIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAEgpsb2dfc3RyZWFtEgR0aW1lEgdtZXNzYWdlMgsQSioHaXN0aG11cw==";
    // Valid base64 that does not decode to a Substrait Plan protobuf.
    private static final String PLAN_MALFORMED = "Zm9vYmFy";
    private static final long TIME_LOWER = 1_700_000_000_000L;
    private static final long TIME_UPPER = 1_700_000_100_000L;

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private List<ByteHolder> mockS3Storage;
    private CloudwatchRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private CloudWatchLogsClient mockAwsLogs;

    @Mock
    private S3Client mockS3;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
    {
        schemaForRead = CloudwatchMetadataHandler.CLOUDWATCH_SCHEMA;

        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new CloudwatchRecordHandler(mockS3, mockSecretsManager, mockAthena, mockAwsLogs, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(mockS3, allocator);

        when(mockS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(mockS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });

        when(mockAwsLogs.getLogEvents(nullable(GetLogEventsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            GetLogEventsRequest request = (GetLogEventsRequest) invocationOnMock.getArguments()[0];

            //Check that predicate pushdown was propagated to cloudwatch
            assertNotNull(request.startTime());
            assertNotNull(request.endTime());

            GetLogEventsResponse.Builder responseBuilder = GetLogEventsResponse.builder();

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

            List<OutputLogEvent> logEvents = new ArrayList<>();
            if (request.nextToken() == null || Integer.valueOf(request.nextToken()) < 3) {
                long continuation = request.nextToken() == null ? 0 : Integer.valueOf(request.nextToken());
                for (int i = 0; i < LOG_EVENTS_PAGE_SIZE; i++) {
                    OutputLogEvent outputLogEvent = OutputLogEvent.builder()
                            .message("message-" + (continuation * i))
                            .timestamp(i * 100L)
                            .build();
                    logEvents.add(outputLogEvent);
                }
            }

            responseBuilder.events(logEvents);
            if (nextToken != null) {
                responseBuilder.nextForwardToken(String.valueOf(nextToken));
            }

            return responseBuilder.build();
        });

        // Mock CloudWatchLogsClient for passthrough
        StartQueryResponse mockStartQueryResponse = StartQueryResponse.builder().queryId("test-query-id").build();
        Mockito.when(mockAwsLogs.startQuery(any(software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest.class))).thenReturn(mockStartQueryResponse);
        GetQueryResultsResponse mockResultsResponse = GetQueryResultsResponse.builder().status(software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus.COMPLETE).build();
        Mockito.when(mockAwsLogs.getQueryResults(any(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class))).thenReturn(mockResultsResponse);
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void doReadRecords_withoutSpill_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_withoutSpill_returnsReadRecordsResponse: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 100L)), false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        ReadRecordsRequest request = createReadRecordsRequest(constraints, BLOCK_SIZE, BLOCK_SIZE);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecords_withoutSpill_returnsReadRecordsResponse: rows[{}]", response.getRecordCount());

        assertEquals(3, response.getRecords().getRowCount());
        logger.info("doReadRecords_withoutSpill_returnsReadRecordsResponse: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("doReadRecords_withoutSpill_returnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withSpill_spillsToMultipleRemoteBlocks()
            throws Exception
    {
        logger.info("doReadRecords_withSpill_spillsToMultipleRemoteBlocks: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.of(
                Range.range(allocator, Types.MinorType.BIGINT.getType(), 100L, true, 100_000_000L, true)));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        ReadRecordsRequest request = createReadRecordsRequest(constraints, 1_500_000L, 0);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecords_withSpill_spillsToMultipleRemoteBlocks: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecords_withSpill_spillsToMultipleRemoteBlocks: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecords_withSpill_spillsToMultipleRemoteBlocks: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }

        logger.info("doReadRecords_withSpill_spillsToMultipleRemoteBlocks: exit");
    }

    @Test
    public void readWithConstraint_withPassthroughArgs_startsCloudwatchQuery() throws InterruptedException, TimeoutException {
        CloudwatchRecordHandler handlerSpy = Mockito.spy(handler);
        BlockSpiller mockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest mockRequest = Mockito.mock(ReadRecordsRequest.class);
        QueryStatusChecker mockChecker = Mockito.mock(QueryStatusChecker.class);
            
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(CloudwatchQueryPassthrough.ENDTIME, "1000");
        passthroughArgs.put(CloudwatchQueryPassthrough.STARTTIME, "0");
        passthroughArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, "fields @message");
        passthroughArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "group1");
        passthroughArgs.put(CloudwatchQueryPassthrough.LIMIT, "1");
        passthroughArgs.put("schemaFunctionName", "SYSTEM.QUERY");
            
        Constraints constraints = createPassthroughConstraints(passthroughArgs);
        Mockito.when(mockRequest.getConstraints()).thenReturn(constraints);

        handlerSpy.readWithConstraint(mockSpiller, mockRequest, mockChecker);
        Mockito.verify(mockAwsLogs).startQuery(any(software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest.class));
    }

    @Test
    public void pushDownConstraints_withEqualTimeConstraint_pushesDownConstraint() throws Exception {
        
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(CloudwatchMetadataHandler.LOG_TIME_FIELD,
                SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                        ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 1000L)), false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        ReadRecordsRequest request = createReadRecordsRequest(constraints, BLOCK_SIZE, 0);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Expected RemoteReadRecordsResponse for equal time constraint",
                  rawResponse instanceof RemoteReadRecordsResponse);
        
        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse;
        assertNotNull("Response should not be null", response);
        assertTrue("Should have at least one remote block", response.getNumberBlocks() > 0);
        assertNotNull("Schema should not be null", response.getSchema());
        
        verify(mockAwsLogs, atLeastOnce()).getLogEvents(argThat((GetLogEventsRequest logRequest) ->
            logRequest.startTime() != null && logRequest.endTime() != null &&
            logRequest.startTime().equals(1000L) && logRequest.endTime().equals(1000L)
        ));
    }

    @Test
    public void pushDownConstraints_withRangeTimeConstraint_pushesDownConstraint() throws Exception {
        
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(CloudwatchMetadataHandler.LOG_TIME_FIELD,
                SortedRangeSet.of(Range.range(allocator, Types.MinorType.BIGINT.getType(), 1000L, true, 5000L, true)));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        ReadRecordsRequest request = createReadRecordsRequest(constraints, BLOCK_SIZE, 0);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Expected RemoteReadRecordsResponse for range time constraint",
                  rawResponse instanceof RemoteReadRecordsResponse);
        
        RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse;
        assertNotNull("Response should not be null", response);
        assertNotNull("Schema should not be null", response.getSchema());
        assertTrue("Should have at least one remote block", response.getNumberBlocks() > 0);
        
        verify(mockAwsLogs, atLeastOnce()).getLogEvents(argThat((GetLogEventsRequest logRequest) ->
            logRequest.startTime() != null && logRequest.endTime() != null &&
            logRequest.startTime().equals(1000L) && logRequest.endTime().equals(5000L)
        ));
    }

    @Test(expected = NullPointerException.class)
    public void doReadRecords_withNullConstraints_throwsException() {
        // ReadRecordsRequest constructor will throw NPE for null constraints
        new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                new TableName("schema", "table"),
                schemaForRead,
                Split.newBuilder(S3SpillLocation.newBuilder()
                                .withBucket(UUID.randomUUID().toString())
                                .withSplitId(UUID.randomUUID().toString())
                                .withQueryId(UUID.randomUUID().toString())
                                .withIsDirectory(true)
                                .build(),
                        keyFactory.create()).add(CloudwatchMetadataHandler.LOG_STREAM_FIELD, "table").build(),
                null,
                BLOCK_SIZE,
                0
        );
    }

    @Test(expected = AthenaConnectorException.class)
    public void readWithConstraint_withPassthroughAndMissingArgs_throwsException() throws InterruptedException, TimeoutException {
        logger.info("readWithConstraint_withPassthroughAndMissingArgs_throwsException: enter");
        
        CloudwatchRecordHandler handlerSpy = Mockito.spy(handler);
        BlockSpiller mockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest mockRequest = Mockito.mock(ReadRecordsRequest.class);
        QueryStatusChecker mockChecker = Mockito.mock(QueryStatusChecker.class);
        
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(CloudwatchQueryPassthrough.ENDTIME, "1000");
        // Missing STARTTIME, QUERYSTRING, LOGGROUPNAMES, LIMIT
        passthroughArgs.put("schemaFunctionName", "SYSTEM.QUERY");
        
        Constraints constraints = createPassthroughConstraints(passthroughArgs);
        Mockito.when(mockRequest.getConstraints()).thenReturn(constraints);

        // Should throw an exception due to missing required arguments
        handlerSpy.readWithConstraint(mockSpiller, mockRequest, mockChecker);
    }

    private ReadRecordsRequest createReadRecordsRequest(Constraints constraints, long maxBlockSize, long maxInlineBlockSize) {
        return new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                new TableName("schema", "table"),
                schemaForRead,
                Split.newBuilder(S3SpillLocation.newBuilder()
                                        .withBucket(UUID.randomUUID().toString())
                                        .withSplitId(UUID.randomUUID().toString())
                                        .withQueryId(UUID.randomUUID().toString())
                                        .withIsDirectory(true)
                                        .build(),
                                keyFactory.create())
                        .add(CloudwatchMetadataHandler.LOG_STREAM_FIELD, "table")
                        .build(),
                constraints,
                maxBlockSize,
                maxInlineBlockSize);
    }

    private Constraints createPassthroughConstraints(Map<String, String> passthroughArgs) {
        return new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, passthroughArgs, null);
    }

    @Test
    public void doReadRecords_withEmptyLogEvents_returnsEmptyRecords()
            throws Exception
    {
        Mockito.doAnswer((InvocationOnMock invocationOnMock) -> {
            GetLogEventsResponse.Builder responseBuilder = GetLogEventsResponse.builder();
            responseBuilder.events(Collections.emptyList());
            responseBuilder.nextForwardToken(null);
            return responseBuilder.build();
        }).when(mockAwsLogs).getLogEvents(nullable(GetLogEventsRequest.class));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 100L)), false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        
        ReadRecordsRequest request = createReadRecordsRequest(constraints, BLOCK_SIZE, BLOCK_SIZE);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        // Should have 0 records when log events are empty
        assertEquals(0, response.getRecords().getRowCount());

        verify(mockAwsLogs, atLeastOnce()).getLogEvents(nullable(GetLogEventsRequest.class));
    }

    @Test
    public void readWithConstraint_withPassthroughEmptyResults_completesWithoutWritingRows() throws InterruptedException, TimeoutException {
        
        CloudwatchRecordHandler handlerSpy = Mockito.spy(handler);
        BlockSpiller mockSpiller = Mockito.mock(BlockSpiller.class);
        ReadRecordsRequest mockRequest = Mockito.mock(ReadRecordsRequest.class);
        QueryStatusChecker mockChecker = Mockito.mock(QueryStatusChecker.class);
            
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(CloudwatchQueryPassthrough.ENDTIME, "1000");
        passthroughArgs.put(CloudwatchQueryPassthrough.STARTTIME, "0");
        passthroughArgs.put(CloudwatchQueryPassthrough.QUERYSTRING, "fields @message");
        passthroughArgs.put(CloudwatchQueryPassthrough.LOGGROUPNAMES, "group1");
        passthroughArgs.put(CloudwatchQueryPassthrough.LIMIT, "1");
        passthroughArgs.put("schemaFunctionName", "SYSTEM.QUERY");
            
        Constraints constraints = createPassthroughConstraints(passthroughArgs);
        Mockito.when(mockRequest.getConstraints()).thenReturn(constraints);

        GetQueryResultsResponse emptyResultsResponse = GetQueryResultsResponse.builder()
                .status(software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus.COMPLETE)
                .results(Collections.emptyList())
                .build();
        
        Mockito.when(mockAwsLogs.getQueryResults(any(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class)))
                .thenReturn(emptyResultsResponse);

        // Should complete without throwing an exception
        handlerSpy.readWithConstraint(mockSpiller, mockRequest, mockChecker);
        
        // Verify query was started and results were fetched
        Mockito.verify(mockAwsLogs).startQuery(any(software.amazon.awssdk.services.cloudwatchlogs.model.StartQueryRequest.class));
        Mockito.verify(mockAwsLogs).getQueryResults(any(software.amazon.awssdk.services.cloudwatchlogs.model.GetQueryResultsRequest.class));
        
        // Verify that writeRows was never called since there are no results to write
        Mockito.verify(mockSpiller, Mockito.never()).writeRows(any());

        assertEquals(software.amazon.awssdk.services.cloudwatchlogs.model.QueryStatus.COMPLETE, emptyResultsResponse.status());
        assertTrue("Results should be empty", emptyResultsResponse.results().isEmpty());
    }

    // ---------------------------------------------------------------------------------------------
    // Managed-connector (Athena federation) behaviour: customer FAS credential override + Substrait
    // time-range pushdown. These tests re-stub getLogEvents with a single-page capturing answer
    // (the @Before stub asserts non-null time bounds and returns a large multi-page response, which
    // is incompatible with the no-pushdown assertions below).
    // ---------------------------------------------------------------------------------------------

    private void stubCapturingGetLogEvents()
    {
        // Use doAnswer(...).when(...) rather than when(...).thenAnswer(...): the latter would re-invoke the
        // asserting answer installed in @Before (with a null request) while evaluating the matcher.
        doAnswer((InvocationOnMock inv) ->
                GetLogEventsResponse.builder()
                        .events(OutputLogEvent.builder().timestamp(100L).message("message-0").build())
                        .build())
                .when(mockAwsLogs).getLogEvents(nullable(GetLogEventsRequest.class));
    }

    private ReadRecordsRequest managedConnectorRequest(Constraints constraints)
    {
        Split split = Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket(UUID.randomUUID().toString())
                        .withSplitId(UUID.randomUUID().toString())
                        .withQueryId(UUID.randomUUID().toString())
                        .withIsDirectory(true)
                        .build(), keyFactory.create())
                .add(CloudwatchMetadataHandler.LOG_GROUP_FIELD, "/glue-connectors/cloudwatch-test")
                .add(CloudwatchMetadataHandler.LOG_STREAM_FIELD, "cloudwatch-test-stream")
                .build();

        return new ReadRecordsRequest(identity,
                "catalog",
                "queryId-" + System.currentTimeMillis(),
                new TableName("/glue-connectors/cloudwatch-test", "cloudwatch-test-stream"),
                schemaForRead,
                split,
                constraints,
                100_000_000_000L,  // large so the single row never spills
                100_000_000_000L);
    }

    private Constraints substraitConstraints(String planBase64)
    {
        return new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                DEFAULT_NO_LIMIT, Collections.emptyMap(), new QueryPlan("", planBase64));
    }

    private GetLogEventsRequest runAndCaptureLogEventsRequest(CloudwatchRecordHandler target, Constraints constraints)
            throws Exception
    {
        RecordResponse response = target.doReadRecords(allocator, managedConnectorRequest(constraints));
        assertTrue(response instanceof ReadRecordsResponse);
        ArgumentCaptor<GetLogEventsRequest> captor = ArgumentCaptor.forClass(GetLogEventsRequest.class);
        verify(mockAwsLogs, atLeastOnce()).getLogEvents(captor.capture());
        return captor.getValue();
    }

    @Test
    public void substraitTimeGreaterThanIsPushedToStartTime()
            throws Exception
    {
        stubCapturingGetLogEvents();
        GetLogEventsRequest request = runAndCaptureLogEventsRequest(handler, substraitConstraints(PLAN_TIME_GT));
        assertEquals(Long.valueOf(TIME_LOWER), request.startTime());
        assertNull(request.endTime());
    }

    @Test
    public void substraitTimeRangeIsPushedToStartAndEndTime()
            throws Exception
    {
        stubCapturingGetLogEvents();
        GetLogEventsRequest request = runAndCaptureLogEventsRequest(handler, substraitConstraints(PLAN_TIME_BETWEEN));
        assertEquals(Long.valueOf(TIME_LOWER), request.startTime());
        assertEquals(Long.valueOf(TIME_UPPER), request.endTime());
    }

    @Test
    public void substraitTimeEqualIsPushedToBothBounds()
            throws Exception
    {
        stubCapturingGetLogEvents();
        GetLogEventsRequest request = runAndCaptureLogEventsRequest(handler, substraitConstraints(PLAN_TIME_EQUAL));
        assertEquals(Long.valueOf(TIME_LOWER), request.startTime());
        assertEquals(Long.valueOf(TIME_LOWER), request.endTime());
    }

    @Test
    public void substraitNonTimePredicateIsNotPushedDown()
            throws Exception
    {
        // A predicate on message has no GetLogEvents equivalent; nothing is pushed and no exception occurs.
        stubCapturingGetLogEvents();
        GetLogEventsRequest request = runAndCaptureLogEventsRequest(handler, substraitConstraints(PLAN_MESSAGE_EQUAL));
        assertNull(request.startTime());
        assertNull(request.endTime());
    }

    @Test
    public void malformedSubstraitPlanDoesNotThrowAndSkipsPushdown()
            throws Exception
    {
        // Best-effort guard: an unparseable plan must never fail the query; it just skips time pushdown.
        stubCapturingGetLogEvents();
        GetLogEventsRequest request = runAndCaptureLogEventsRequest(handler, substraitConstraints(PLAN_MALFORMED));
        assertNull(request.startTime());
        assertNull(request.endTime());
    }

    @Test
    public void appliesCustomerFasOverrideToCloudwatchCalls()
            throws Exception
    {
        // On the managed-connector path getRequestOverrideConfig(configOptions) yields the customer FAS
        // credentials. Verify that override is threaded onto the Cloudwatch GetLogEvents call.
        stubCapturingGetLogEvents();
        CloudwatchRecordHandler spyHandler = spy(handler);
        AwsRequestOverrideConfiguration fasOverride = AwsRequestOverrideConfiguration.builder().build();
        doReturn(fasOverride).when(spyHandler).getRequestOverrideConfig(anyMap());

        // Legacy (non-Substrait) constraints: null queryPlan and empty summary => no time pushdown, but
        // the override must still be applied.
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        GetLogEventsRequest request = runAndCaptureLogEventsRequest(spyHandler, constraints);
        assertTrue("expected customer FAS override on the Cloudwatch request",
                request.overrideConfiguration().isPresent());
        assertSame(fasOverride, request.overrideConfiguration().get());
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
