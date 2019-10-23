package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
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
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class GlueMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(MockitoJUnitRunner.class);

    private String accountId = IdentityUtil.fakeIdentity().getAccount();
    private String queryId = "queryId";
    private String catalog = "default";
    private String schema = "database1";
    private String table = "table1";

    private GlueMetadataHandler handler;

    private BlockAllocatorImpl allocator;

    @Mock
    private AWSGlue mockGlue;

    @Before
    public void setUp()
            throws Exception
    {
        handler = new GlueMetadataHandler(mockGlue, "glue-test")
        {
            @Override
            protected GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            protected GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            protected Field convertField(String name, String type)
            {
                if ("int".equals(type)) {
                    return FieldBuilder.newBuilder(name, Types.MinorType.INT.getType()).build();
                }
                else if ("bigint".equals(type)) {
                    return FieldBuilder.newBuilder(name, Types.MinorType.BIGINT.getType()).build();
                }
                else if ("string".equals(type)) {
                    return FieldBuilder.newBuilder(name, Types.MinorType.VARCHAR.getType()).build();
                }
                throw new IllegalArgumentException("Unsupported type " + type);
            }
        };
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
            throws Exception
    {
        logger.info("doListSchemaNames: enter");

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
        logger.info("doListSchemaNames: exit");
    }

    @Test
    public void doListTables()
            throws Exception
    {
        logger.info("doListTables - enter");

        List<Table> tables = new ArrayList<>();
        tables.add(new Table().withName("table1"));
        tables.add(new Table().withName("table2"));

        when(mockGlue.getTables(any(GetTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    GetTablesRequest request = (GetTablesRequest) invocationOnMock.getArguments()[0];
                    assertEquals(accountId, request.getCatalogId());
                    assertEquals(schema, request.getDatabaseName());
                    GetTablesResult mockResult = mock(GetTablesResult.class);
                    if (request.getNextToken() == null) {
                        when(mockResult.getTableList()).thenReturn(tables);
                        when(mockResult.getNextToken()).thenReturn("next");
                    }
                    else {
                        //only return real info on 1st call
                        when(mockResult.getTableList()).thenReturn(new ArrayList<>());
                        when(mockResult.getNextToken()).thenReturn(null);
                    }
                    return mockResult;
                });

        ListTablesRequest req = new ListTablesRequest(IdentityUtil.fakeIdentity(), queryId, catalog, schema);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        Set<String> tableNames = tables.stream().map(next -> next.getName()).collect(Collectors.toSet());
        for (TableName next : res.getTables()) {
            assertEquals(schema, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        logger.info("doGetTable - enter");

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("param1", "val1");
        expectedParams.put("param2", "val2");

        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int").withComment("comment"));
        columns.add(new Column().withName("col2").withType("bigint").withComment("comment"));
        columns.add(new Column().withName("col3").withType("string").withComment("comment"));

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

        assertTrue(res.getSchema().getFields().size() > 0);
        assertTrue(res.getSchema().getCustomMetadata().size() > 0);

        logger.info("doGetTable - exit");
    }
}