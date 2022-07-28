package com.amazonaws.athena.connector.lambda.handlers;

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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.GET_TABLES_REQUEST_MAX_RESULTS;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.getSourceTableName;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.populateSourceTableNameIfAvailable;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlueMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(GlueMetadataHandlerTest.class);

    private String accountId = IdentityUtil.fakeIdentity().getAccount();
    private String queryId = "queryId";
    private String catalog = "default";
    private String schema = "database1";
    private String table = "table1";

    private GlueMetadataHandler handler;

    private BlockAllocatorImpl allocator;

    // The following list is purposely unordered to demonstrate that the doListTables pagination logic does not
    // consider the order of the tables deterministic (i.e. pagination will work irrespective of the order that the
    // tables are returned from the source).
    private final List<Table> unPaginatedTables = new ImmutableList.Builder<Table>()
            .add(new Table().withName("table3"))
            .add(new Table().withName("table2"))
            .add(new Table().withName("table5"))
            .add(new Table().withName("table4"))
            .add(new Table().withName("table1"))
            .build();

    // The following response is expected be returned from doListTables when the pagination pageSize is greater than
    // the number of tables in the unPaginatedTables list (or pageSize has UNLIMITED_PAGE_SIZE_VALUE).
    private final ListTablesResponse fullListResponse = new ListTablesResponse(catalog,
            new ImmutableList.Builder<TableName>()
                    .add(new TableName(schema, "table1"))
                    .add(new TableName(schema, "table2"))
                    .add(new TableName(schema, "table3"))
                    .add(new TableName(schema, "table4"))
                    .add(new TableName(schema, "table5"))
                    .build(), null);

    @Rule
    public TestName testName = new TestName();

    @Mock
    private AWSGlue mockGlue;

    @Mock
    private Context mockContext;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        handler = new GlueMetadataHandler(mockGlue,
                new LocalKeyFactory(),
                mock(AWSSecretsManager.class),
                mock(AmazonAthena.class),
                "glue-test",
                "spill-bucket",
                "spill-prefix")
        {
            @Override
            public GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
                    throws Exception
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
            {
                throw new UnsupportedOperationException();
            }
        };
        allocator = new BlockAllocatorImpl();

        // doListTables pagination.
        when(mockGlue.getTables(any(GetTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    GetTablesRequest request = (GetTablesRequest) invocationOnMock.getArguments()[0];
                    String nextToken = request.getNextToken();
                    int pageSize = request.getMaxResults() == null ? UNLIMITED_PAGE_SIZE_VALUE : request.getMaxResults();
                    assertEquals(accountId, request.getCatalogId());
                    assertEquals(schema, request.getDatabaseName());
                    GetTablesResult mockResult = mock(GetTablesResult.class);
                    if (pageSize == UNLIMITED_PAGE_SIZE_VALUE) {
                        // Simulate full list of tables returned from Glue.
                        when(mockResult.getTableList()).thenReturn(unPaginatedTables);
                        when(mockResult.getNextToken()).thenReturn(null);
                    }
                    else {
                        // Simulate paginated list of tables returned from Glue.
                        List<Table> paginatedTables = unPaginatedTables.stream()
                                .sorted(Comparator.comparing(Table::getName))
                                .filter(table -> nextToken == null || table.getName().compareTo(nextToken) >= 0)
                                .limit(pageSize + 1)
                                .collect(Collectors.toList());
                        if (paginatedTables.size() > pageSize) {
                            when(mockResult.getNextToken()).thenReturn(paginatedTables.get(pageSize).getName());
                            when(mockResult.getTableList()).thenReturn(paginatedTables.subList(0, pageSize));
                        }
                        else {
                            when(mockResult.getNextToken()).thenReturn(null);
                            when(mockResult.getTableList()).thenReturn(paginatedTables);
                        }
                    }
                    return mockResult;
                });
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
            throws Exception
    {
        List<Database> databases = new ArrayList<>();
        databases.add(new Database().withName("db1"));
        databases.add(new Database().withName("db2"));

        when(mockGlue.getDatabases(any(GetDatabasesRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    GetDatabasesRequest request = (GetDatabasesRequest) invocationOnMock.getArguments()[0];
                    assertEquals(accountId, request.getCatalogId());
                    GetDatabasesResult mockResult = mock(GetDatabasesResult.class);
                    if (request.getNextToken() == null) {
                        when(mockResult.getDatabaseList()).thenReturn(databases);
                        when(mockResult.getNextToken()).thenReturn("next");
                    }
                    else {
                        //only return real info on 1st call
                        when(mockResult.getDatabaseList()).thenReturn(new ArrayList<>());
                        when(mockResult.getNextToken()).thenReturn(null);
                    }
                    return mockResult;
                });

        ListSchemasRequest req = new ListSchemasRequest(IdentityUtil.fakeIdentity(), queryId, catalog);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertEquals(databases.stream().map(next -> next.getName()).collect(Collectors.toList()),
                new ArrayList<>(res.getSchemas()));

        verify(mockGlue, times(2)).getDatabases(any(GetDatabasesRequest.class));
    }

    @Test
    public void doListTablesWithUnlimitedPageSize()
            throws Exception
    {
        ListTablesRequest req = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                queryId, catalog, schema, null, UNLIMITED_PAGE_SIZE_VALUE);
        logger.info("Request - {}", req);
        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEquals("Lists do not match.", fullListResponse, actualResponse);
    }

    @Test
    public void doListTablesWithLargePageSize()
            throws Exception
    {
        ListTablesRequest req = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                queryId, catalog, schema, null, GET_TABLES_REQUEST_MAX_RESULTS + 50);
        logger.info("Request - {}", req);
        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEquals("Lists do not match.", fullListResponse, actualResponse);
    }

    @Test
    public void doListTablesWithPagination()
            throws Exception
    {
        logger.info("First paginated request");
        ListTablesRequest req = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                queryId, catalog, schema, null, 3);
        logger.info("Request - {}", req);
        ListTablesResponse expectedResponse = new ListTablesResponse(req.getCatalogName(),
                new ImmutableList.Builder<TableName>()
                        .add(new TableName(req.getSchemaName(), "table1"))
                        .add(new TableName(req.getSchemaName(), "table2"))
                        .add(new TableName(req.getSchemaName(), "table3"))
                        .build(), "table4");
        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEquals("Lists do not match.", expectedResponse, actualResponse);

        logger.info("Second paginated request");
        req = new ListTablesRequest(IdentityUtil.fakeIdentity(),
                queryId, catalog, schema, actualResponse.getNextToken(), 3);
        logger.info("Request - {}", req);
        expectedResponse = new ListTablesResponse(req.getCatalogName(),
                new ImmutableList.Builder<TableName>()
                        .add(new TableName(req.getSchemaName(), "table4"))
                        .add(new TableName(req.getSchemaName(), "table5"))
                        .build(), null);
        actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEquals("Lists do not match.", expectedResponse, actualResponse);
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        String sourceTable = "My-Table";

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(SOURCE_TABLE_PROPERTY, sourceTable);
        expectedParams.put(COLUMN_NAME_MAPPING_PROPERTY, "col2=Col2,col3=Col3, col4=Col4");
        expectedParams.put(DATETIME_FORMAT_MAPPING_PROPERTY, "col2=someformat2, col1=someformat1 ");

        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int").withComment("comment"));
        columns.add(new Column().withName("col2").withType("bigint").withComment("comment"));
        columns.add(new Column().withName("col3").withType("string").withComment("comment"));
        columns.add(new Column().withName("col4").withType("timestamp").withComment("comment"));
        columns.add(new Column().withName("col5").withType("date").withComment("comment"));
        columns.add(new Column().withName("col6").withType("timestamptz").withComment("comment"));
        columns.add(new Column().withName("col7").withType("timestamptz").withComment("comment"));

        List<Column> partitionKeys = new ArrayList<>();
        columns.add(new Column().withName("partition_col1").withType("int").withComment("comment"));

        Table mockTable = mock(Table.class);
        StorageDescriptor mockSd = mock(StorageDescriptor.class);

        when(mockTable.getName()).thenReturn(table);
        when(mockTable.getStorageDescriptor()).thenReturn(mockSd);
        when(mockTable.getParameters()).thenReturn(expectedParams);
        when(mockSd.getColumns()).thenReturn(columns);

        when(mockGlue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    com.amazonaws.services.glue.model.GetTableRequest request =
                            (com.amazonaws.services.glue.model.GetTableRequest) invocationOnMock.getArguments()[0];

                    assertEquals(accountId, request.getCatalogId());
                    assertEquals(schema, request.getDatabaseName());
                    assertEquals(table, request.getName());

                    GetTableResult mockResult = mock(GetTableResult.class);
                    when(mockResult.getTable()).thenReturn(mockTable);
                    return mockResult;
                });

        GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table));
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res);

        assertTrue(res.getSchema().getFields().size() == 8);
        assertTrue(res.getSchema().getCustomMetadata().size() > 0);
        assertTrue(res.getSchema().getCustomMetadata().containsKey(DATETIME_FORMAT_MAPPING_PROPERTY));
        assertEquals(res.getSchema().getCustomMetadata().get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), "Col2=someformat2,col1=someformat1");
        assertEquals(sourceTable, getSourceTableName(res.getSchema()));

        //Verify column name mapping works
        assertNotNull(res.getSchema().findField("partition_col1"));
        assertNotNull(res.getSchema().findField("col1"));
        assertNotNull(res.getSchema().findField("Col2"));
        assertNotNull(res.getSchema().findField("Col3"));
        assertNotNull(res.getSchema().findField("Col4"));
        assertNotNull(res.getSchema().findField("col5"));
        assertNotNull(res.getSchema().findField("col6"));
        assertNotNull(res.getSchema().findField("col7"));

        //Verify types
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("partition_col1").getType()).equals(Types.MinorType.INT));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("col1").getType()).equals(Types.MinorType.INT));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("Col2").getType()).equals(Types.MinorType.BIGINT));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("Col3").getType()).equals(Types.MinorType.VARCHAR));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("Col4").getType()).equals(Types.MinorType.DATEMILLI));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("col5").getType()).equals(Types.MinorType.DATEDAY));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("col6").getType()).equals(Types.MinorType.TIMESTAMPMILLITZ));
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("col7").getType()).equals(Types.MinorType.TIMESTAMPMILLITZ));
    }

    @Test
    public void populateSourceTableFromLocation() {
        Map<String, String> params = new HashMap<>();
        List<String> partitions = Arrays.asList("aws", "aws-cn", "aws-us-gov");
        for (String partition : partitions) {
            StorageDescriptor storageDescriptor = new StorageDescriptor().withLocation(String.format("arn:%s:dynamodb:us-east-1:012345678910:table/My-Table", partition));
            Table table = new Table().withParameters(params).withStorageDescriptor(storageDescriptor);
            SchemaBuilder schemaBuilder = new SchemaBuilder();
            populateSourceTableNameIfAvailable(table, schemaBuilder);
            Schema schema = schemaBuilder.build();
            assertEquals("My-Table", getSourceTableName(schema));
        }
    }

    @Test
    public void doGetTableEmptyComment()
            throws Exception
    {
        String sourceTable = "My-Table";

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put(SOURCE_TABLE_PROPERTY, sourceTable);
        // Put in a conflicting parameter
        expectedParams.put("col1", "col1");

        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int").withComment(" "));

        Table mockTable = mock(Table.class);
        StorageDescriptor mockSd = mock(StorageDescriptor.class);

        when(mockTable.getName()).thenReturn(table);
        when(mockTable.getStorageDescriptor()).thenReturn(mockSd);
        when(mockTable.getParameters()).thenReturn(expectedParams);
        when(mockSd.getColumns()).thenReturn(columns);

        when(mockGlue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    com.amazonaws.services.glue.model.GetTableRequest request =
                            (com.amazonaws.services.glue.model.GetTableRequest) invocationOnMock.getArguments()[0];

                    assertEquals(accountId, request.getCatalogId());
                    assertEquals(schema, request.getDatabaseName());
                    assertEquals(table, request.getName());

                    GetTableResult mockResult = mock(GetTableResult.class);
                    when(mockResult.getTable()).thenReturn(mockTable);
                    return mockResult;
                });

        GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table));
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res);

        //Verify column name mapping works
        assertNotNull(res.getSchema().findField("col1"));

        //Verify types
        assertTrue(Types.getMinorTypeForArrowType(res.getSchema().findField("col1").getType()).equals(Types.MinorType.INT));
    }

    @Test
    public void testGetCatalog() {
        // Catalog should be the account from the request
        MetadataRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table));
        String catalog = handler.getCatalog(req);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);

        // Catalog should be the account from the lambda context's function arn
        when(mockContext.getInvokedFunctionArn())
                .thenReturn("arn:aws:lambda:us-east-1:012345678912:function:athena-123");
        req.setContext(mockContext);
        catalog = handler.getCatalog(req);
        assertEquals("012345678912", catalog);

        // Catalog should be the account from the request since function arn is invalid
        when(mockContext.getInvokedFunctionArn())
                .thenReturn("arn:aws:lambda:us-east-1:012345678912:function:");
        req.setContext(mockContext);
        catalog = handler.getCatalog(req);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);

        // Catalog should be the account from the request since function arn is null
        when(mockContext.getInvokedFunctionArn())
                .thenReturn(null);
        req.setContext(mockContext);
        catalog = handler.getCatalog(req);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);
    }
}
