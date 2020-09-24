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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
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
    private Jedis mockClient;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private JedisPoolFactory mockFactory;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        when(mockFactory.getOrCreateConn(eq(decodedEndpoint))).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);

        when(amazonS3.putObject(anyObject(), anyObject(), anyObject(), anyObject()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = (InputStream) invocationOnMock.getArguments()[2];
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    mockS3Storage.add(byteHolder);
                    return mock(PutObjectResult.class);
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    S3Object mockObject = mock(S3Object.class);
                    ByteHolder byteHolder = mockS3Storage.get(0);
                    mockS3Storage.remove(0);
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
        when(mockClient.scan(anyString(), any(ScanParams.class))).then((InvocationOnMock invocationOnMock) -> {
            String cursor = (String) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("1", result);
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("0", result);
            }
        });

        AtomicLong value = new AtomicLong(0);
        when(mockClient.get(anyString()))
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
        when(mockClient.scan(anyString(), any(ScanParams.class))).then((InvocationOnMock invocationOnMock) -> {
            String cursor = (String) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("1", result);
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("0", result);
            }
        });

        //4 columns per key
        AtomicLong intColVal = new AtomicLong(0);
        when(mockClient.hgetAll(anyString())).then((InvocationOnMock invocationOnMock) -> {
            Map<String, String> result = new HashMap<>();
            result.put("intcol", String.valueOf(intColVal.getAndIncrement()));
            result.put("stringcol", UUID.randomUUID().toString());
            result.put("extracol", UUID.randomUUID().toString());
            return result;
        });

        AtomicLong value = new AtomicLong(0);
        when(mockClient.get(anyString()))
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
        when(mockClient.scan(anyString(), any(ScanParams.class))).then((InvocationOnMock invocationOnMock) -> {
            String cursor = (String) invocationOnMock.getArguments()[0];
            if (cursor == null || cursor.equals("0")) {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("1", result);
            }
            else {
                List<String> result = new ArrayList<>();
                result.add(UUID.randomUUID().toString());
                return new ScanResult<>("0", result);
            }
        });

        //4 rows per key
        when(mockClient.zscan(anyString(), anyString())).then((InvocationOnMock invocationOnMock) -> {
            String cursor = (String) invocationOnMock.getArguments()[1];
            if (cursor == null || cursor.equals("0")) {
                List<Tuple> result = new ArrayList<>();
                result.add(new Tuple("1", 0.0D));
                result.add(new Tuple("2", 0.0D));
                result.add(new Tuple("3", 0.0D));
                return new ScanResult<>("1", result);
            }
            else {
                List<Tuple> result = new ArrayList<>();
                result.add(new Tuple("4", 0.0D));
                return new ScanResult<>("0", result);
            }
        });

        AtomicLong value = new AtomicLong(0);
        when(mockClient.get(anyString()))
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
