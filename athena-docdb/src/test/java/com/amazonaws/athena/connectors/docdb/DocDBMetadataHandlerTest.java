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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");
    private String catalog = "default";
    private DocDBMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private AWSGlue awsGlue;

    @Mock
    private AWSSecretsManager secretsManager;

    @Before
    public void setUp()
            throws Exception
    {
        when(connectionFactory.getOrCreateConn(anyString())).thenReturn(mockClient);

        handler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, "spillBucket", "spillPrefix");
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
    {
        logger.info("doListSchemaNames: enter");

        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1");
        schemaNames.add("schema2");
        schemaNames.add("schema3");

        when(mockClient.listDatabaseNames()).thenReturn(StubbingCursor.iterate(schemaNames));

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());
        assertEquals(schemaNames, new ArrayList<>(res.getSchemas()));

        logger.info("doListSchemaNames: exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        String schema = "schema1";

        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        when(mockClient.getDatabase(eq(schema))).thenReturn(mockDatabase);
        when(mockDatabase.listCollectionNames()).thenReturn(StubbingCursor.iterate(tableNames));

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default", schema);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        for (TableName next : res.getTables()) {
            assertEquals(schema, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());

        logger.info("doListTables - exit");
    }

    /**
     * TODO: Add more types.
     */
    @Test
    public void doGetTable()
            throws Exception
    {
        logger.info("doGetTable - enter");

        String schema = "schema1";
        String table = "table1";

        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("stringCol", "stringVal");
        doc1.put("intCol", 1);
        doc1.put("doubleCol", 2.2D);
        doc1.put("longCol", 100L);

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
        when(mockClient.getDatabase(eq(schema))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(table))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = new GetTableRequest(identity, "queryId", catalog, new TableName(schema, table));
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(8, res.getSchema().getFields().size());

        Field stringCol = res.getSchema().findField("stringCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = res.getSchema().findField("stringCol2");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = res.getSchema().findField("intCol");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = res.getSchema().findField("intCol2");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = res.getSchema().findField("doubleCol");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = res.getSchema().findField("doubleCol2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = res.getSchema().findField("longCol");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = res.getSchema().findField("longCol2");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout()
    {
        logger.info("doGetTableLayout - enter");

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName("schema1", "table1"),
                new Constraints(new HashMap<>()),
                new HashMap<>());

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);

        logger.info("doGetTableLayout: partitions[{}]", partitions.getRowCount());
    }

    @Test
    public void doGetSplits()
    {
        logger.info("doGetSplits: enter");

        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(identity,
                "queryId",
                "catalog_name",
                new TableName("schema", "table_name"),
                partitions,
                partitionCols,
                new Constraints(new HashMap<>()),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == 1);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        logger.info("doGetSplits: exit");
    }
}
