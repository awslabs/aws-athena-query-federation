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
import com.amazonaws.services.s3.AmazonS3;
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
import org.junit.Test;
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
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");
    private String catalog = "default";
    private String conStr = "connectionString";
    private DocDBRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private AmazonS3 amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private AWSSecretsManager mockSecretsManager;

    @Before
    public void setUp()
    {
        logger.info("setUpBefore - enter");

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
                .addStructField("struct")
                .addChildField("struct", "struct_string", Types.MinorType.VARCHAR.getType())
                .addChildField("struct", "struct_int", Types.MinorType.INT.getType())
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();

        when(connectionFactory.getOrCreateConn(anyString())).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(AmazonS3.class);

        when(amazonS3.putObject(anyObject(), anyObject(), anyObject(), anyObject()))
                .thenAnswer(new Answer<Object>()
                {
                    @Override
                    public Object answer(InvocationOnMock invocationOnMock)
                            throws Throwable
                    {
                        InputStream inputStream = (InputStream) invocationOnMock.getArguments()[2];
                        DocDBRecordHandlerTest.ByteHolder byteHolder = new ByteHolder();
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

        handler = new DocDBRecordHandler(amazonS3, mockSecretsManager, connectionFactory);
        spillReader = new S3BlockSpillReader(amazonS3, allocator);

        logger.info("setUpBefore - exit");
    }

    @After
    public void after()
    {
        allocator.close();
    }

    private Document makeDocument(Schema schema, int seed)
    {
        Document doc = new Document();
        doc.put("stringCol", "stringVal");
        doc.put("intCol", 1);
        doc.put("col3", 22.0D);
        return doc;
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        logger.info("doReadRecordsNoSpill: enter");

        String schema = "schema1";
        String table = "table1";

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

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(schema))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(table))).thenReturn(mockCollection);
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

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                catalog,
                "queryId-" + System.currentTimeMillis(),
                new TableName(schema, table),
                schemaForRead,
                Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, conStr).build(),
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

        logger.info("doReadRecordsNoSpill: exit");
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        logger.info("doReadRecordsSpill: enter");

        String schema = "schema1";
        String table = "table1";

        List<Document> documents = new ArrayList<>();

        for (int docNum = 0; docNum < 20_000; docNum++) {
            documents.add(DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum));
        }

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(schema))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(table))).thenReturn(mockCollection);
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

        ReadRecordsRequest request = new ReadRecordsRequest(identity,
                catalog,
                "queryId-" + System.currentTimeMillis(),
                new TableName(schema, table),
                schemaForRead,
                Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, conStr).build(),
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
