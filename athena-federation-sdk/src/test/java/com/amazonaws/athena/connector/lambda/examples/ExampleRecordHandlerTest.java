package com.amazonaws.athena.connector.lambda.examples;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RecordService;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.serde.ObjectMapperUtil;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExampleRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandlerTest.class);

    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private RecordService recordService;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private AWSSecretsManager awsSecretsManager;
    private AmazonAthena athena;
    private S3BlockSpillReader spillReader;
    private BlockAllocatorImpl allocator;
    private Schema schemaForRead;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("year", new ArrowType.Int(32, true))
                .addField("month", new ArrowType.Int(32, true))
                .addField("day", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                .addField("int", Types.MinorType.INT.getType())
                .addField("tinyint", Types.MinorType.TINYINT.getType())
                .addField("smallint", Types.MinorType.SMALLINT.getType())
                .addField("bigint", Types.MinorType.BIGINT.getType())
                .addField("float4", Types.MinorType.FLOAT4.getType())
                .addField("float8", Types.MinorType.FLOAT8.getType())
                .addField("bit", Types.MinorType.BIT.getType())
                .addField("varchar", Types.MinorType.VARCHAR.getType())
                .addField("varbinary", Types.MinorType.VARBINARY.getType())
                .addField("datemilli", Types.MinorType.DATEMILLI.getType())
                .addField("dateday", Types.MinorType.DATEDAY.getType())
                .addField("decimal", new ArrowType.Decimal(10, 2))
                .addField("decimalLong", new ArrowType.Decimal(36, 2)) //Example of a List of Structs
                .addField(
                        FieldBuilder.newBuilder("list", new ArrowType.List())
                                .addField(
                                        FieldBuilder.newBuilder("innerStruct", Types.MinorType.STRUCT.getType())
                                                .addStringField("varchar")
                                                .addBigIntField("bigint")
                                                .build())
                                .build())
                //Example of a List Of Lists
                .addField(
                        FieldBuilder.newBuilder("outerlist", new ArrowType.List())
                                .addListField("innerList", Types.MinorType.VARCHAR.getType())
                                .build())
                .addField(FieldBuilder.newBuilder("simplemap", new ArrowType.Map(false))
                        .addField("entries", Types.MinorType.STRUCT.getType(), false, Arrays.asList(
                                FieldBuilder.newBuilder("key", Types.MinorType.VARCHAR.getType(), false).build(),
                                FieldBuilder.newBuilder("value", Types.MinorType.INT.getType()).build()))
                        .build())
                .addMetadata("partitionCols", "year,month,day")
                .build();

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);
        awsSecretsManager = mock(AWSSecretsManager.class);
        athena = mock(AmazonAthena.class);

        when(amazonS3.putObject(anyObject()))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        InputStream inputStream = ((PutObjectRequest) invocationOnMock.getArguments()[0]).getInputStream();
                        ByteHolder byteHolder = new ByteHolder();
                        byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                        mockS3Storage.add(byteHolder);
                        return mock(PutObjectResult.class);
                    }
                });

        when(amazonS3.getObject(anyString(), anyString()))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        S3Object mockObject = mock(S3Object.class);
                        ByteHolder byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        when(mockObject.getObjectContent()).thenReturn(
                                new S3ObjectInputStream(
                                        new ByteArrayInputStream(byteHolder.getBytes()), null));
                        return mockObject;
                    }
                });

        recordService = new LocalHandler(allocator, amazonS3, awsSecretsManager, athena);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doReadRecordsNoSpill()
    {
        logger.info("doReadRecordsNoSpill: enter");
        for (int i = 0; i < 2; i++) {
            EncryptionKey encryptionKey = (i % 2 == 0) ? keyFactory.create() : null;
            logger.info("doReadRecordsNoSpill: Using encryptionKey[" + encryptionKey + "]");

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                    ImmutableList.of(Range.equal(allocator, Types.MinorType.FLOAT8.getType(), 22.0D)), false));

            ReadRecordsRequest request = new ReadRecordsRequest(IdentityUtil.fakeIdentity(),
                    "catalog",
                    "queryId-" + System.currentTimeMillis(),
                    new TableName("schema", "table"),
                    schemaForRead,
                    Split.newBuilder(makeSpillLocation(), encryptionKey).add("year", "10").add("month", "10").add("day", "10").build(),
                    new Constraints(constraintsMap),
                    100_000_000_000L, //100GB don't expect this to spill
                    100_000_000_000L
            );
            ObjectMapperUtil.assertSerialization(request);

            RecordResponse rawResponse = recordService.readRecords(request);
            ObjectMapperUtil.assertSerialization(rawResponse);

            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
            logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

            assertTrue(response.getRecords().getRowCount() == 1);
            logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        }
        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");
        for (int i = 0; i < 2; i++) {
            EncryptionKey encryptionKey = (i % 2 == 0) ? keyFactory.create() : null;
            logger.info("doReadRecordsSpill: Using encryptionKey[" + encryptionKey + "]");

            Map<String, ValueSet> constraintsMap = new HashMap<>();
            constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                    ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));
            constraintsMap.put("unknown", EquatableValueSet.newBuilder(allocator, Types.MinorType.FLOAT8.getType(), false, true).add(1.1D).build());
            constraintsMap.put("unknown2", new AllOrNoneValueSet(Types.MinorType.FLOAT8.getType(), false, true));

            ReadRecordsRequest request = new ReadRecordsRequest(IdentityUtil.fakeIdentity(),
                    "catalog",
                    "queryId-" + System.currentTimeMillis(),
                    new TableName("schema", "table"),
                    schemaForRead,
                    Split.newBuilder(makeSpillLocation(), encryptionKey).add("year", "10").add("month", "10").add("day", "10").build(),
                    new Constraints(constraintsMap),
                    1_600_000L, //~1.5MB so we should see some spill
                    1000L
            );
            ObjectMapperUtil.assertSerialization(request);

            RecordResponse rawResponse = recordService.readRecords(request);
            ObjectMapperUtil.assertSerialization(rawResponse);

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
        }
        logger.info("doReadRecordsSpill: exit");
    }

    private static class LocalHandler
            implements RecordService
    {
        private ExampleRecordHandler handler;
        private final BlockAllocatorImpl allocator;

        public LocalHandler(BlockAllocatorImpl allocator, AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena)
        {
            handler = new ExampleRecordHandler(amazonS3, secretsManager, athena);
            handler.setNumRows(20_000);//lower number for faster unit tests vs integ tests
            this.allocator = allocator;
        }

        @Override
        public RecordResponse readRecords(RecordRequest request)
        {

            try {
                switch (request.getRequestType()) {
                    case READ_RECORDS:
                        ReadRecordsRequest req = (ReadRecordsRequest) request;
                        RecordResponse response = handler.doReadRecords(allocator, req);
                        return response;
                    default:
                        throw new RuntimeException("Unknown request type " + request.getRequestType());
                }
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private SpillLocation makeSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                .withBucket("athena-virtuoso-test")
                .withPrefix("lambda-spill")
                .withQueryId(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
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
