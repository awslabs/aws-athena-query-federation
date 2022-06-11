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
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CloudwatchRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchRecordHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private List<ByteHolder> mockS3Storage;
    private CloudwatchRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private BlockAllocator allocator;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private AWSLogs mockAwsLogs;

    @Mock
    private AmazonS3 mockS3;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        schemaForRead = CloudwatchMetadataHandler.CLOUDWATCH_SCHEMA;

        mockS3Storage = new ArrayList<>();
        allocator = new BlockAllocatorImpl();
        handler = new CloudwatchRecordHandler(mockS3, mockSecretsManager, mockAthena, mockAwsLogs);
        spillReader = new S3BlockSpillReader(mockS3, allocator);

        when(mockS3.putObject(anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return mock(PutObjectResult.class);
                });

        when(mockS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    when(mockObject.getObjectContent()).thenReturn(
                            new S3ObjectInputStream(
                                    new ByteArrayInputStream(byteHolder.getBytes()), null));
                    return mockObject;
                });

        when(mockAwsLogs.getLogEvents(any(GetLogEventsRequest.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            GetLogEventsRequest request = (GetLogEventsRequest) invocationOnMock.getArguments()[0];

            //Check that predicate pushdown was propagated to cloudwatch
            assertNotNull(request.getStartTime());
            assertNotNull(request.getEndTime());

            GetLogEventsResult result = new GetLogEventsResult();

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

            List<OutputLogEvent> logEvents = new ArrayList<>();
            if (request.getNextToken() == null || Integer.valueOf(request.getNextToken()) < 3) {
                long continuation = request.getNextToken() == null ? 0 : Integer.valueOf(request.getNextToken());
                for (int i = 0; i < 100_000; i++) {
                    OutputLogEvent outputLogEvent = new OutputLogEvent();
                    outputLogEvent.setMessage("message-" + (continuation * i));
                    outputLogEvent.setTimestamp(i * 100L);
                    logEvents.add(outputLogEvent);
                }
            }

            result.withEvents(logEvents);
            if (nextToken != null) {
                result.setNextForwardToken(String.valueOf(nextToken));
            }

            return result;
        });
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 100L)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
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
                new Constraints(constraintsMap),
                100_000_000_000L,
                100_000_000_000L//100GB don't expect this to spill
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() == 3);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("time", SortedRangeSet.of(
                Range.range(allocator, Types.MinorType.BIGINT.getType(), 100L, true, 100_000_000L, true)));

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
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
                new Constraints(constraintsMap),
                1_500_000L, //~1.5MB so we should see some spill
                0
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }

        logger.info("doReadRecordsSpill: exit");
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
