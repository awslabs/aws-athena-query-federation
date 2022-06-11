/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.Block;
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
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
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

import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandlerTest.class);

    private DocDBRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private DocDBMetadataHandler mdHandler;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Mock
    private AWSGlue awsGlue;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    MongoDatabase mockDatabase;

    @Mock
    MongoCollection mockCollection;

    @Mock
    FindIterable mockIterable;

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                .addField("int", Types.MinorType.INT.getType())
                .addField("tinyint", Types.MinorType.TINYINT.getType())
                .addField("smallint", Types.MinorType.SMALLINT.getType())
                .addField("bigint", Types.MinorType.BIGINT.getType())
                .addField("uint1", Types.MinorType.UINT1.getType())
                .addField("uint2", Types.MinorType.UINT2.getType())
                .addField("uint4", Types.MinorType.UINT4.getType())
                .addField("uint8", Types.MinorType.UINT8.getType())
                .addField("float4", Types.MinorType.FLOAT4.getType())
                .addField("float8", Types.MinorType.FLOAT8.getType())
                .addField("bit", Types.MinorType.BIT.getType())
                .addField("varchar", Types.MinorType.VARCHAR.getType())
                .addField("varbinary", Types.MinorType.VARBINARY.getType())
                .addField("decimal", new ArrowType.Decimal(10, 2))
                .addField("decimalLong", new ArrowType.Decimal(36, 2))
                .addField("unsupported", Types.MinorType.VARCHAR.getType())
                .addStructField("struct")
                .addChildField("struct", "struct_string", Types.MinorType.VARCHAR.getType())
                .addChildField("struct", "struct_int", Types.MinorType.INT.getType())
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();

        when(connectionFactory.getOrCreateConn(anyString())).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);
        mockDatabase = mock(MongoDatabase.class);
        mockCollection = mock(MongoCollection.class);
        mockIterable = mock(FindIterable.class);

        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);

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

        handler = new DocDBRecordHandler(amazonS3, mockSecretsManager, mockAthena, connectionFactory);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
        mdHandler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena, "spillBucket", "spillPrefix");
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        int docNum = 11;
        Document doc1 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc1);
        doc1.put("col3", 22.0D);

        Document doc2 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc2);
        doc2.put("col3", 22.0D);

        Document doc3 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc3);
        doc3.put("col3", 21.0D);
        doc3.put("unsupported",new UnsupportedType());

        when(mockCollection.find(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.FLOAT8.getType(), 22.0D)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, CONNECTION_STRING).build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() == 2);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        for (int docNum = 0; docNum < 20_000; docNum++) {
            documents.add(DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum));
        }

        when(mockCollection.find(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, CONNECTION_STRING).build(),
                new Constraints(constraintsMap),
                1_500_000L, //~1.5MB so we should see some spill
                0L
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
    }

    @Test
    public void nestedStructTest()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document result = new Document();
        documents.add(result);

        Document listStruct1 = new Document();
        listStruct1.put("SomeSubStruct", "someSubStruct1");
        List<Document> subList = new ArrayList<>();
        Document listSubStruct1 = new Document();
        listSubStruct1.put("SomeSubSubStruct", "someSubSubStruct");
        subList.add(listSubStruct1);
        listStruct1.put("SomeSubList", subList);
        Document listStruct2 = new Document();
        listStruct2.put("SomeSubStruct1", "someSubStruct2");
        List<Document> list = new ArrayList<>();
        list.add(listStruct1);
        list.add(listStruct1);
        Document structWithList = new Document();
        structWithList.put("SomeList", list);
        Document structWithNullList = new Document();
        structWithNullList.put("SomeNullList", null);

        Document simpleSubStruct = new Document();
        simpleSubStruct.put("SomeSimpleSubStruct", "someSimpleSubStruct");
        structWithList.put("SimpleSubStruct", simpleSubStruct);
        structWithList.put("SimpleSubStructNullList", structWithNullList);

        result.put("ComplexStruct", structWithList);

        Document simpleStruct = new Document();
        simpleStruct.put("SomeSimpleStruct", "someSimpleStruct");
        result.put("SimpleStruct", simpleStruct);

        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME);
        GetTableResponse res = mdHandler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        when(mockCollection.find(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(any(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));


        Map<String, ValueSet> constraintsMap = new HashMap<>();
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                res.getSchema(),
                Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, CONNECTION_STRING).build(),
                new Constraints(constraintsMap),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecordCount() == 1);
        String expectedString = "[ComplexStruct : {[SomeList : {{[SomeSubStruct : someSubStruct1]," +
                "[SomeSubList : {{[SomeSubSubStruct : someSubSubStruct]}}]}," +
                "{[SomeSubStruct : someSubStruct1],[SomeSubList : {{[SomeSubSubStruct : someSubSubStruct]}}]}}]," +
                "[SimpleSubStruct : {[SomeSimpleSubStruct : someSimpleSubStruct]}]," +
                "[SimpleSubStructNullList : {[SomeNullList : null]}]}], [SimpleStruct : {[SomeSimpleStruct : someSimpleStruct]}]";
        assertEquals(expectedString, BlockUtils.rowToString(response.getRecords(), 0));
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
