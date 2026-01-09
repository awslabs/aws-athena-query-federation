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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
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
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBMetadataHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandlerTest.class);

    private static final String STRING_COL = "stringCol";
    private static final String STRING_COL2 = "stringCol2";
    private static final String INT_COL = "intCol";
    private static final String INT_COL2 = "intCol2";
    private static final String DOUBLE_COL = "doubleCol";
    private static final String DOUBLE_COL2 = "doubleCol2";
    private static final String LONG_COL = "longCol";
    private static final String LONG_COL2 = "longCol2";
    private static final String BOOLEAN_COL = "booleanCol";
    private static final String BOOLEAN_COL2 = "booleanCol2";
    private static final String FLOAT_COL = "floatCol";
    private static final String FLOAT_COL2 = "floatCol2";
    private static final String DATE_COL = "dateCol";
    private static final String DATE_COL2 = "dateCol2";
    private static final String TIMESTAMP_COL = "timestampCol";
    private static final String TIMESTAMP_COL2 = "timestampCol2";
    private static final String OBJECT_ID_COL = "objectIdCol";
    private static final String OBJECT_ID_COL2 = "objectIdCol2";
    
    private static final String STRING_VALUE = "stringVal";
    private static final double DOUBLE_VALUE = 2.2D;
    private static final int INT_VALUE = 1;
    private static final long LONG_VALUE = 100L;
    private static final float FLOAT_VALUE1 = 1.5F;
    private static final float FLOAT_VALUE2 = 2.5F;
    private static final float FLOAT_VALUE3 = 3.5F;
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";
    private static final String TABLE4 = "table4";
    private static final String TABLE5 = "table5";
    private static final String CURSOR_KEY = "cursor";
    private static final String FIRST_BATCH_KEY = "firstBatch";
    private static final String NAME_KEY = "name";
    private static final String UNSUPPORTED_FIELD = "unsupported";
    private static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";
    private static final String CASE_INSENSITIVE_MATCH_ENABLED = Boolean.TRUE.toString();
    private static final String MIXED_CASE_SCHEMA = "deFAULT";
    private static final String MIXED_CASE_TABLE = "tesT_Table";
    private static final int PAGE_SIZE_TWO = 2;
    private static final int EXPECTED_FIELD_COUNT = 19;
    private static final String SYSTEM_QUERY = "system.query";
    private static final String EXAMPLE_DATABASE = "example";
    private static final String TPCDS_COLLECTION = "tpcds";
    private static final String TITLE_FIELD = "title";
    private static final String YEAR_FIELD = "year";
    private static final String TYPE_FIELD = "type";
    private static final String ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    private static final String QUERY_PASSTHROUGH_ENABLED = Boolean.TRUE.toString();
    private static final String SCHEMA_FUNCTION_NAME = "schemaFunctionName";

    private DocDBMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private GlueClient awsGlue;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        when(connectionFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        handler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena, SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of());
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
    public void doListSchemaNames() throws Exception
    {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1");
        schemaNames.add("schema2");
        schemaNames.add("schema3");

        when(mockClient.listDatabaseNames()).thenReturn(StubbingCursor.iterate(schemaNames));

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());
        assertEquals(schemaNames, new ArrayList<>(res.getSchemas()));
    }

    @Test
    public void doListTables() throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add(TABLE1);
        tableNames.add(TABLE2);
        tableNames.add(TABLE3);

        Document tableNamesDocument = new Document(CURSOR_KEY,
                new Document(FIRST_BATCH_KEY,
                        Arrays.asList(new Document(NAME_KEY, TABLE1),
                                new Document(NAME_KEY, TABLE2),
                                new Document(NAME_KEY, TABLE3))));

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.runCommand(any())).thenReturn(tableNamesDocument);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                null, UNLIMITED_PAGE_SIZE_VALUE);

        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        for (TableName next : res.getTables()) {
            assertEquals(DEFAULT_SCHEMA, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put(STRING_COL, STRING_VALUE);
        doc1.put(INT_COL, INT_VALUE);
        doc1.put(DOUBLE_COL, DOUBLE_VALUE);
        doc1.put(LONG_COL, LONG_VALUE);
        doc1.put(BOOLEAN_COL, true);
        doc1.put(FLOAT_COL, FLOAT_VALUE1);
        doc1.put(DATE_COL, new Date());
        doc1.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc1.put(OBJECT_ID_COL, ObjectId.get());
        doc1.put(UNSUPPORTED_FIELD, new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put(STRING_COL2, STRING_VALUE);
        doc2.put(INT_COL2, INT_VALUE);
        doc2.put(DOUBLE_COL2, DOUBLE_VALUE);
        doc2.put(LONG_COL2, LONG_VALUE);
        doc2.put(BOOLEAN_COL2, false);
        doc2.put(FLOAT_COL2, FLOAT_VALUE2);
        doc2.put(DATE_COL2, new Date());
        doc2.put(TIMESTAMP_COL2, new BsonTimestamp(System.currentTimeMillis()));
        doc2.put(OBJECT_ID_COL2, ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put(STRING_COL, STRING_VALUE);
        doc3.put(INT_COL2, INT_VALUE);
        doc3.put(DOUBLE_COL, DOUBLE_VALUE);
        doc3.put(LONG_COL2, LONG_VALUE);
        doc3.put(BOOLEAN_COL, false);
        doc3.put(FLOAT_COL, FLOAT_VALUE3);
        doc3.put(DATE_COL, new Date());
        doc3.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc3.put(OBJECT_ID_COL, ObjectId.get());

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

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(EXPECTED_FIELD_COUNT, res.getSchema().getFields().size());

        assertMinorType(res.getSchema(), STRING_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), STRING_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), INT_COL, Types.MinorType.INT);
        assertMinorType(res.getSchema(), INT_COL2, Types.MinorType.INT);
        assertMinorType(res.getSchema(), DOUBLE_COL, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), DOUBLE_COL2, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), LONG_COL, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), LONG_COL2, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), BOOLEAN_COL, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), BOOLEAN_COL2, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), FLOAT_COL, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), FLOAT_COL2, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), DATE_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), DATE_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), OBJECT_ID_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), OBJECT_ID_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), UNSUPPORTED_FIELD, Types.MinorType.VARCHAR);
    }

    @Test
    public void doGetTableCaseInsensitiveMatch()
            throws Exception
    {

        DocDBMetadataHandler caseInsensitiveHandler = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED));
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put(STRING_COL, STRING_VALUE);
        doc1.put(INT_COL, INT_VALUE);
        doc1.put(DOUBLE_COL, DOUBLE_VALUE);
        doc1.put(LONG_COL, LONG_VALUE);
        doc1.put(BOOLEAN_COL, true);
        doc1.put(FLOAT_COL, FLOAT_VALUE1);
        doc1.put(DATE_COL, new Date());
        doc1.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc1.put(OBJECT_ID_COL, ObjectId.get());
        doc1.put(UNSUPPORTED_FIELD, new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put(STRING_COL2, STRING_VALUE);
        doc2.put(INT_COL2, INT_VALUE);
        doc2.put(DOUBLE_COL2, DOUBLE_VALUE);
        doc2.put(LONG_COL2, LONG_VALUE);
        doc2.put(BOOLEAN_COL2, false);
        doc2.put(FLOAT_COL2, FLOAT_VALUE2);
        doc2.put(DATE_COL2, new Date());
        doc2.put(TIMESTAMP_COL2, new BsonTimestamp(System.currentTimeMillis()));
        doc2.put(OBJECT_ID_COL2, ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put(STRING_COL, STRING_VALUE);
        doc3.put(INT_COL2, INT_VALUE);
        doc3.put(DOUBLE_COL, DOUBLE_VALUE);
        doc3.put(LONG_COL2, LONG_VALUE);
        doc3.put(BOOLEAN_COL, false);
        doc3.put(FLOAT_COL, FLOAT_VALUE3);
        doc3.put(DATE_COL, new Date());
        doc3.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc3.put(OBJECT_ID_COL, ObjectId.get());

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);

        MongoIterable mockListDatabaseNamesIterable = mock(MongoIterable.class);
        when(mockClient.listDatabaseNames()).thenReturn(mockListDatabaseNamesIterable);

        when(mockListDatabaseNamesIterable.spliterator()).thenReturn(ImmutableList.of(DEFAULT_SCHEMA).spliterator());

        MongoIterable mockListCollectionsNamesIterable = mock(MongoIterable.class);
        when(mockDatabase.listCollectionNames()).thenReturn(mockListCollectionsNamesIterable);
        when(mockListCollectionsNamesIterable.spliterator()).thenReturn(ImmutableList.of(TEST_TABLE).spliterator());

        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        TableName tableNameInput = new TableName("DEfault", TEST_TABLE.toUpperCase());
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        GetTableResponse res = caseInsensitiveHandler.doGetTable(allocator, req);

        assertEquals(DEFAULT_SCHEMA, res.getTableName().getSchemaName());
        assertEquals(TEST_TABLE, res.getTableName().getTableName());
        logger.info("doGetTable - {}", res);

        assertEquals(EXPECTED_FIELD_COUNT, res.getSchema().getFields().size());

        assertMinorType(res.getSchema(), STRING_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), STRING_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), INT_COL, Types.MinorType.INT);
        assertMinorType(res.getSchema(), INT_COL2, Types.MinorType.INT);
        assertMinorType(res.getSchema(), DOUBLE_COL, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), DOUBLE_COL2, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), LONG_COL, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), LONG_COL2, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), BOOLEAN_COL, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), BOOLEAN_COL2, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), FLOAT_COL, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), FLOAT_COL2, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), DATE_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), DATE_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), OBJECT_ID_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), OBJECT_ID_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), UNSUPPORTED_FIELD, Types.MinorType.VARCHAR);
    }


    @Test
    public void doGetTableCaseInsensitiveMatchMultipleMatch()
            throws Exception
    {

        DocDBMetadataHandler caseInsensitiveHandler = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of(ENABLE_CASE_INSENSITIVE_MATCH, CASE_INSENSITIVE_MATCH_ENABLED));

        MongoIterable mockListDatabaseNamesIterable = mock(MongoIterable.class);
        when(mockClient.listDatabaseNames()).thenReturn(mockListDatabaseNamesIterable);
        when(mockListDatabaseNamesIterable.spliterator()).thenReturn(ImmutableList.of(DEFAULT_SCHEMA, "DEFAULT").spliterator());

        TableName tableNameInput = new TableName(MIXED_CASE_SCHEMA, TEST_TABLE.toUpperCase());
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        try {
            GetTableResponse res = caseInsensitiveHandler.doGetTable(allocator, req);
            fail("doGetTableCaseInsensitiveMatchMultipleMatch should failed");
        } catch(IllegalArgumentException ex){
            assertEquals("Schema name is empty or more than 1 for case insensitive match. schemaName: deFAULT, size: 2", ex.getMessage());
        }
    }

    @Test
    public void doGetTableCaseInsensitiveMatchNotEnable()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put(STRING_COL, STRING_VALUE);
        doc1.put(INT_COL, INT_VALUE);
        doc1.put(DOUBLE_COL, DOUBLE_VALUE);
        doc1.put(LONG_COL, LONG_VALUE);
        doc1.put(BOOLEAN_COL, true);
        doc1.put(FLOAT_COL, FLOAT_VALUE1);
        doc1.put(DATE_COL, new Date());
        doc1.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc1.put(OBJECT_ID_COL, ObjectId.get());
        doc1.put(UNSUPPORTED_FIELD, new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put(STRING_COL2, STRING_VALUE);
        doc2.put(INT_COL2, INT_VALUE);
        doc2.put(DOUBLE_COL2, DOUBLE_VALUE);
        doc2.put(LONG_COL2, LONG_VALUE);
        doc2.put(BOOLEAN_COL2, false);
        doc2.put(FLOAT_COL2, FLOAT_VALUE2);
        doc2.put(DATE_COL2, new Date());
        doc2.put(TIMESTAMP_COL2, new BsonTimestamp(System.currentTimeMillis()));
        doc2.put(OBJECT_ID_COL2, ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put(STRING_COL, STRING_VALUE);
        doc3.put(INT_COL2, INT_VALUE);
        doc3.put(DOUBLE_COL, DOUBLE_VALUE);
        doc3.put(LONG_COL2, LONG_VALUE);
        doc3.put(BOOLEAN_COL, false);
        doc3.put(FLOAT_COL, FLOAT_VALUE3);
        doc3.put(DATE_COL, new Date());
        doc3.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc3.put(OBJECT_ID_COL, ObjectId.get());

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(MIXED_CASE_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(MIXED_CASE_TABLE))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        TableName tableNameInput = new TableName(MIXED_CASE_SCHEMA, MIXED_CASE_TABLE);
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertEquals(MIXED_CASE_SCHEMA, res.getTableName().getSchemaName());
        assertEquals(MIXED_CASE_TABLE, res.getTableName().getTableName());
        assertEquals(EXPECTED_FIELD_COUNT, res.getSchema().getFields().size());

        assertMinorType(res.getSchema(), STRING_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), STRING_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), INT_COL, Types.MinorType.INT);
        assertMinorType(res.getSchema(), INT_COL2, Types.MinorType.INT);
        assertMinorType(res.getSchema(), DOUBLE_COL, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), DOUBLE_COL2, Types.MinorType.FLOAT8);
        assertMinorType(res.getSchema(), LONG_COL, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), LONG_COL2, Types.MinorType.BIGINT);
        assertMinorType(res.getSchema(), BOOLEAN_COL, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), BOOLEAN_COL2, Types.MinorType.BIT);
        assertMinorType(res.getSchema(), FLOAT_COL, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), FLOAT_COL2, Types.MinorType.FLOAT4);
        assertMinorType(res.getSchema(), DATE_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), DATE_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), TIMESTAMP_COL2, Types.MinorType.DATEMILLI);
        assertMinorType(res.getSchema(), OBJECT_ID_COL, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), OBJECT_ID_COL2, Types.MinorType.VARCHAR);
        assertMinorType(res.getSchema(), UNSUPPORTED_FIELD, Types.MinorType.VARCHAR);

        verify(mockClient, Mockito.never()).listDatabaseNames();
        verify(mockDatabase, Mockito.never()).listCollectionNames();

    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = new GetTableLayoutRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
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
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                partitionCols,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
    }

    @Test
    public void doListTables_withPagination_returnsPaginatedResults() throws Exception
    {
        Document tableNamesDocument = new Document(CURSOR_KEY,
                new Document(FIRST_BATCH_KEY,
                        Arrays.asList(new Document(NAME_KEY, TABLE1),
                                new Document(NAME_KEY, TABLE2),
                                new Document(NAME_KEY, TABLE3),
                                new Document(NAME_KEY, TABLE4),
                                new Document(NAME_KEY, TABLE5))));

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.runCommand(any())).thenReturn(tableNamesDocument);

        // First request - page size 2, no token
        ListTablesRequest firstRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                null, PAGE_SIZE_TWO);
        ListTablesResponse firstResponse = handler.doListTables(allocator, firstRequest);

        // Verify first page
        assertEquals("Should return 2 tables for first page", PAGE_SIZE_TWO, firstResponse.getTables().size());
        List<TableName> firstPageTables = new ArrayList<>(firstResponse.getTables());
        assertEquals(new TableName(DEFAULT_SCHEMA, TABLE1), firstPageTables.get(0));
        assertEquals(new TableName(DEFAULT_SCHEMA, TABLE2), firstPageTables.get(1));
        assertEquals("2", firstResponse.getNextToken());

        // Second request - page size 2, token "2"
        ListTablesRequest secondRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                firstResponse.getNextToken(), PAGE_SIZE_TWO);
        ListTablesResponse secondResponse = handler.doListTables(allocator, secondRequest);

        // Verify second page
        assertEquals("Should return 2 tables for second page", PAGE_SIZE_TWO, secondResponse.getTables().size());
        List<TableName> secondPageTables = new ArrayList<>(secondResponse.getTables());
        assertEquals(new TableName(DEFAULT_SCHEMA, TABLE3), secondPageTables.get(0));
        assertEquals(new TableName(DEFAULT_SCHEMA, TABLE4), secondPageTables.get(1));
        assertEquals("4", secondResponse.getNextToken());

        // Final request - page size 2, token "4"
        ListTablesRequest finalRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                secondResponse.getNextToken(), PAGE_SIZE_TWO);
        ListTablesResponse finalResponse = handler.doListTables(allocator, finalRequest);

        // Verify final page
        assertEquals("Should return 1 table for final page", 1, finalResponse.getTables().size());
        List<TableName> finalPageTables = new ArrayList<>(finalResponse.getTables());
        assertEquals(new TableName(DEFAULT_SCHEMA, TABLE5), finalPageTables.get(0));
        assertEquals("6", finalResponse.getNextToken());

        // Verify empty page after all data is consumed
        ListTablesRequest emptyRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                finalResponse.getNextToken(), PAGE_SIZE_TWO);
        ListTablesResponse emptyResponse = handler.doListTables(allocator, emptyRequest);
        assertEquals("Should return 0 tables when all data is consumed", 0, emptyResponse.getTables().size());
        assertEquals("8", emptyResponse.getNextToken());
    }

    @Test
    public void doGetDataSourceCapabilities_withQueryPassthroughEnabled_returnsCapabilities()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(ENABLE_QUERY_PASSTHROUGH, QUERY_PASSTHROUGH_ENABLED);

        DocDBMetadataHandler handlerWithQPT = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse response = handlerWithQPT.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertFalse("Capabilities should not be empty when query passthrough is enabled", response.getCapabilities().isEmpty());
    }

    @Test
    public void doGetQueryPassthroughSchema_withValidParameters_returnsSchema() throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put(TITLE_FIELD, "Bill of Rights");
        doc1.put(YEAR_FIELD, 1791);
        doc1.put(TYPE_FIELD, "document");

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(EXAMPLE_DATABASE))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TPCDS_COLLECTION))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, String> queryPassthroughParameters = new HashMap<>();
        queryPassthroughParameters.put(SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        queryPassthroughParameters.put("DATABASE", EXAMPLE_DATABASE);
        queryPassthroughParameters.put("COLLECTION", TPCDS_COLLECTION);
        queryPassthroughParameters.put("FILTER", "{\"title\": \"Bill of Rights\"}");
        queryPassthroughParameters.put(ENABLE_QUERY_PASSTHROUGH, QUERY_PASSTHROUGH_ENABLED);

        GetTableRequest request = new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                new TableName(EXAMPLE_DATABASE, TPCDS_COLLECTION),
                queryPassthroughParameters
        );
        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        // Verify response
        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        // Verify that the schema and table names from query passthrough are used
        assertEquals(new TableName(EXAMPLE_DATABASE, TPCDS_COLLECTION), response.getTableName());

        Schema schema = response.getSchema();
        assertNotNull("Schema should not be null", schema);

        // Verify schema fields match the document structure
        assertMinorType(schema, TITLE_FIELD, Types.MinorType.VARCHAR);
        assertMinorType(schema, YEAR_FIELD, Types.MinorType.INT);
        assertMinorType(schema, TYPE_FIELD, Types.MinorType.VARCHAR);
    }

    @Test
    public void doGetQueryPassthroughSchema_withInvalidParameters_throwsAthenaConnectorException()
            throws Exception
    {
        // Test with missing required parameters
        Map<String, String> invalidParams = new HashMap<>();
        invalidParams.put(SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        invalidParams.put(ENABLE_QUERY_PASSTHROUGH, QUERY_PASSTHROUGH_ENABLED);
        // Missing DATABASE and COLLECTION parameters
        GetTableRequest invalidRequest = new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                invalidParams
        );
        try {
            handler.doGetQueryPassthroughSchema(allocator, invalidRequest);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Missing Query Passthrough Argument",
                    ex.getMessage().contains("Missing Query Passthrough Argument"));
        }
    }

    private void assertMinorType(Schema schema, String field, Types.MinorType expected)
    {
        Field f = schema.findField(field);
        assertNotNull(f);
        assertEquals(expected, Types.getMinorTypeForArrowType(f.getType()));
    }
}
