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
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBMetadataHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandlerTest.class);

    private DocDBMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private AWSGlue awsGlue;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        when(connectionFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        handler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of(DEFAULT_CATALOG, "asdfConnectionString"));
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames()
    {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1");
        schemaNames.add("schema2");
        schemaNames.add("schema3");

        when(mockClient.listDatabaseNames()).thenReturn(StubbingCursor.iterate(schemaNames));

        ListSchemasRequest req = ListSchemasRequest.newBuilder().setIdentity(IDENTITY).setQueryId(QUERY_ID).setCatalogName(DEFAULT_CATALOG).build();
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemasList());
        assertEquals(schemaNames, new ArrayList<>(res.getSchemasList()));
    }

    @Test
    public void doListTables()
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.listCollectionNames()).thenReturn(StubbingCursor.iterate(tableNames));

        ListTablesRequest req = ListTablesRequest.newBuilder().setIdentity(IDENTITY).setQueryId(QUERY_ID).setCatalogName(DEFAULT_CATALOG).setSchemaName(DEFAULT_SCHEMA).setPageSize(UNLIMITED_PAGE_SIZE_VALUE).build();
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTablesList());

        for (TableName next : res.getTablesList()) {
            assertEquals(DEFAULT_SCHEMA, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTablesList().size());
    }

    /**
     * TODO: Add more types.
     */
    @Test
    public void doGetTable()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("stringCol", "stringVal");
        doc1.put("intCol", 1);
        doc1.put("doubleCol", 2.2D);
        doc1.put("longCol", 100L);
        doc1.put("unsupported", new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put("stringCol2", "stringVal");
        doc2.put("intCol2", 1);
        doc2.put("doubleCol2", 2.2D);
        doc2.put("longCol2", 100L);

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put("stringCol", "stringVal");
        doc3.put("intCol2", 1);
        doc3.put("doubleCol", 2.2D);
        doc3.put("longCol2", 100L);

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = GetTableRequest.newBuilder().setIdentity(IDENTITY).setQueryId(QUERY_ID).setCatalogName(DEFAULT_CATALOG).setTableName(TABLE_NAME).build();
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(9, ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).getFields().size());

        Field stringCol = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("stringCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("stringCol2");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("intCol");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("intCol2");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("doubleCol");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("doubleCol2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("longCol");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("longCol2");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        Field unsupported = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema()).findField("unsupported");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(unsupported.getType()));
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = GetTableLayoutRequest.newBuilder().setIdentity(IDENTITY).setQueryId(QUERY_ID).setCatalogName(DEFAULT_CATALOG)
        .setTableName(TABLE_NAME).setConstraints(ProtobufMessageConverter.toProtoConstraints(new Constraints(new HashMap<>())))
        .setSchema(ProtobufMessageConverter.toProtoSchemaBytes(schema))
        .build();
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = ProtobufMessageConverter.fromProtoBlock(allocator, res.getPartitions());
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);
    }

    @Test
    public void doGetSplits()
    {
        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, PARTITION_ID, Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest req = GetSplitsRequest.newBuilder().setIdentity(IDENTITY).setQueryId(QUERY_ID).setCatalogName(DEFAULT_CATALOG)
        .setTableName(TABLE_NAME)
        .setPartitions(ProtobufMessageConverter.toProtoBlock(partitions))
        .addAllPartitionColumns(partitionCols)
        .build();
        
        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplitsList().size()});

        assertTrue("Continuation criteria violated", response.getSplitsList().size() == 1);
        assertFalse("Continuation criteria violated", response.hasContinuationToken());
    }
}
