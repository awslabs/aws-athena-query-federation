/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisCommandsWrapper;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionWrapper;
import com.amazonaws.athena.connectors.redis.util.MockKeyScanCursor;
import com.amazonaws.athena.connectors.redis.util.MockScoredValueScanCursor;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValue;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_COLUMN_NAME;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_PREFIX_TABLE_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.KEY_TYPE;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.REDIS_ENDPOINT_PROP;
import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.VALUE_TYPE_TABLE_PROP;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisRecordHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(RedisRecordHandlerTest.class);

    private String endpoint = "${endpoint}";
    private String decodedEndpoint = "endpoint:123";
    private RedisRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private S3BlockSpillReader spillReader;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Rule
    public TestName testName = new TestName();

    @Mock
    private RedisConnectionWrapper<String, String> mockConnection;

    @Mock
    private RedisCommandsWrapper<String, String> mockSyncCommands;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private RedisConnectionFactory mockFactory;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        when(mockFactory.getOrCreateConn(eq(decodedEndpoint), anyBoolean(), anyBoolean(), anyString())).thenReturn(mockConnection);
        when(mockConnection.sync()).thenReturn(mockSyncCommands);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);

        when(amazonS3.putObject(anyObject()))
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

        when(amazonS3.getObject(anyString(), anyString()))
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

        when(mockSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgumentAt(0, GetSecretValueRequest.class);
                    if ("endpoint".equalsIgnoreCase(request.getSecretId())) {
                        return new GetSecretValueResult().withSecretString(decodedEndpoint);
                    }
                    throw new RuntimeException("Unknown secret " + request.getSecretId());
                });

        handler = new RedisRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockFactory);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsLiteral()
            throws Exception
    {
        //4 keys per prefix
        when(mockSyncCommands.scan(any(ScanCursor.class), any(ScanArgs.class))).then((InvocationOnMock invocationOnMock) -> {
            ScanCursor cursor = (ScanCursor) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.getCursor().equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("1");
                scanCursor.setKeys(result);
                return scanCursor;
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("0");
                scanCursor.setKeys(result);
                scanCursor.setFinished(true);
                return scanCursor;
            }
        });

        AtomicLong value = new AtomicLong(0);
        when(mockSyncCommands.get(anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> String.valueOf(value.getAndIncrement()));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, keyFactory.create())
                .add(REDIS_ENDPOINT_PROP, endpoint)
                .add(KEY_TYPE, KeyType.PREFIX.getId())
                .add(KEY_PREFIX_TABLE_PROP, "key-*")
                .add(VALUE_TYPE_TABLE_PROP, ValueType.LITERAL.getId())
                .build();

        Schema schemaForRead = SchemaBuilder.newBuilder()
                .addField("_key_", Types.MinorType.VARCHAR.getType())
                .addField("intcol", Types.MinorType.INT.getType())
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("intcol", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                split,
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsLiteral: rows[{}]", response.getRecordCount());

        logger.info("doReadRecordsLiteral: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecords().getRowCount() == 2);

        FieldReader keyReader = response.getRecords().getFieldReader(KEY_COLUMN_NAME);
        keyReader.setPosition(0);
        assertNotNull(keyReader.readText().toString());

        FieldReader intCol = response.getRecords().getFieldReader("intcol");
        intCol.setPosition(0);
        assertNotNull(intCol.readInteger());
    }

    @Test
    public void doReadRecordsHash()
            throws Exception
    {
        //4 keys per prefix
        when(mockSyncCommands.scan(any(ScanCursor.class), any(ScanArgs.class))).then((InvocationOnMock invocationOnMock) -> {
            ScanCursor cursor = (ScanCursor) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.getCursor().equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("1");
                scanCursor.setKeys(result);
                return scanCursor;
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("0");
                scanCursor.setKeys(result);
                scanCursor.setFinished(true);
                return scanCursor;
            }
        });

        //4 columns per key
        AtomicLong intColVal = new AtomicLong(0);
        when(mockSyncCommands.hgetall(anyString())).then((InvocationOnMock invocationOnMock) -> {
            Map<String, String> result = new HashMap<>();
            result.put("intcol", String.valueOf(intColVal.getAndIncrement()));
            result.put("stringcol", UUID.randomUUID().toString());
            result.put("extracol", UUID.randomUUID().toString());
            return result;
        });

        AtomicLong value = new AtomicLong(0);
        when(mockSyncCommands.get(anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> String.valueOf(value.getAndIncrement()));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, keyFactory.create())
                .add(REDIS_ENDPOINT_PROP, endpoint)
                .add(KEY_TYPE, KeyType.PREFIX.getId())
                .add(KEY_PREFIX_TABLE_PROP, "key-*")
                .add(VALUE_TYPE_TABLE_PROP, ValueType.HASH.getId())
                .build();

        Schema schemaForRead = SchemaBuilder.newBuilder()
                .addField("_key_", Types.MinorType.VARCHAR.getType())
                .addField("intcol", Types.MinorType.INT.getType())
                .addField("stringcol", Types.MinorType.VARCHAR.getType())
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("intcol", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                split,
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsHash: rows[{}]", response.getRecordCount());

        logger.info("doReadRecordsHash: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecords().getRowCount() == 5);
        assertTrue(response.getRecords().getFields().size() == schemaForRead.getFields().size());

        FieldReader keyReader = response.getRecords().getFieldReader(KEY_COLUMN_NAME);
        keyReader.setPosition(0);
        assertNotNull(keyReader.readText());

        FieldReader intCol = response.getRecords().getFieldReader("intcol");
        intCol.setPosition(0);
        assertNotNull(intCol.readInteger());

        FieldReader stringCol = response.getRecords().getFieldReader("stringcol");
        stringCol.setPosition(0);
        assertNotNull(stringCol.readText());
    }

    @Test
    public void doReadRecordsZset()
            throws Exception
    {
        //4 keys per prefix
        when(mockSyncCommands.scan(any(ScanCursor.class), any(ScanArgs.class))).then((InvocationOnMock invocationOnMock) -> {
            ScanCursor cursor = (ScanCursor) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.getCursor().equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("1");
                scanCursor.setKeys(result);
                return scanCursor;
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                MockKeyScanCursor<String> scanCursor = new MockKeyScanCursor<>();
                scanCursor.setCursor("0");
                scanCursor.setKeys(result);
                scanCursor.setFinished(true);
                return scanCursor;
            }
        });

        //4 rows per key
        when(mockSyncCommands.zscan(anyString(), any(ScanCursor.class))).then((InvocationOnMock invocationOnMock) -> {
            ScanCursor cursor = (ScanCursor) invocationOnMock.getArguments()[1];
            if (cursor == null || cursor.getCursor().equals("0")) {
                List<ScoredValue<String>> result = new ArrayList<>();
                result.add(ScoredValue.just(0.0D, "1"));
                result.add(ScoredValue.just(0.0D, "2"));
                result.add(ScoredValue.just(0.0D, "3"));
                MockScoredValueScanCursor<String> scanCursor = new MockScoredValueScanCursor<>();
                scanCursor.setCursor("1");
                scanCursor.setValues(result);
                return scanCursor;
            }
            else {
                List<ScoredValue<String>> result = new ArrayList<>();
                result.add(ScoredValue.just(0.0D, "4"));
                MockScoredValueScanCursor<String> scanCursor = new MockScoredValueScanCursor<>();
                scanCursor.setCursor("0");
                scanCursor.setValues(result);
                scanCursor.setFinished(true);
                return scanCursor;
            }
        });

        AtomicLong value = new AtomicLong(0);
        when(mockSyncCommands.get(anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> String.valueOf(value.getAndIncrement()));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, keyFactory.create())
                .add(REDIS_ENDPOINT_PROP, endpoint)
                .add(KEY_TYPE, KeyType.PREFIX.getId())
                .add(KEY_PREFIX_TABLE_PROP, "key-*")
                .add(VALUE_TYPE_TABLE_PROP, ValueType.ZSET.getId())
                .build();

        Schema schemaForRead = SchemaBuilder.newBuilder()
                .addField("_key_", Types.MinorType.VARCHAR.getType())
                .addField("intcol", Types.MinorType.INT.getType())
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("intcol", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                split,
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsZset: rows[{}]", response.getRecordCount());

        logger.info("doReadRecordsZset: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecords().getRowCount() == 12);

        FieldReader keyReader = response.getRecords().getFieldReader(KEY_COLUMN_NAME);
        keyReader.setPosition(0);
        assertNotNull(keyReader.readText());

        FieldReader intCol = response.getRecords().getFieldReader("intcol");
        intCol.setPosition(0);
        assertNotNull(intCol.readInteger());
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
