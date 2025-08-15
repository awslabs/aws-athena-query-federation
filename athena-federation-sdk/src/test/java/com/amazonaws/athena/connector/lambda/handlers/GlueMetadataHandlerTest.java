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
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.paginators.GetDatabasesIterable;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import static org.mockito.ArgumentMatchers.nullable;
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
            .add(Table.builder().name("table3").build())
            .add(Table.builder().name("table2").build())
            .add(Table.builder().name("table5").build())
            .add(Table.builder().name("table4").build())
            .add(Table.builder().name("table1").build())
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
    private GlueClient mockGlue;

    @Mock
    private Context mockContext;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());
        handler = new GlueMetadataHandler(mockGlue,
                new LocalKeyFactory(),
                mock(SecretsManagerClient.class),
                mock(AthenaClient.class),
                "glue-test",
                "spill-bucket",
                "spill-prefix",
                com.google.common.collect.ImmutableMap.of())
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

            @Override
            public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request) {
                throw new UnsupportedOperationException();
            }
        };
        allocator = new BlockAllocatorImpl();

        // doListTables pagination.
        when(mockGlue.getTables(nullable(GetTablesRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    GetTablesRequest request = (GetTablesRequest) invocationOnMock.getArguments()[0];
                    String nextToken = request.nextToken();
                    int pageSize = request.maxResults() == null ? UNLIMITED_PAGE_SIZE_VALUE : request.maxResults();
                    assertEquals(accountId, request.catalogId());
                    assertEquals(schema, request.databaseName());
                    GetTablesResponse response;
                    if (pageSize == UNLIMITED_PAGE_SIZE_VALUE) {
                        // Simulate full list of tables returned from Glue.
                        response = GetTablesResponse.builder()
                                .tableList(unPaginatedTables)
                                .nextToken(null)
                                .build();
                    }
                    else {
                        // Simulate paginated list of tables returned from Glue.
                        List<Table> paginatedTables = unPaginatedTables.stream()
                                .sorted(Comparator.comparing(Table::name))
                                .filter(table -> nextToken == null || table.name().compareTo(nextToken) >= 0)
                                .limit(pageSize + 1)
                                .collect(Collectors.toList());
                        if (paginatedTables.size() > pageSize) {
                            response = GetTablesResponse.builder()
                                    .tableList(paginatedTables.subList(0, pageSize))
                                    .nextToken(paginatedTables.get(pageSize).name())
                                    .build();
                        }
                        else {
                            response = GetTablesResponse.builder()
                                    .tableList(paginatedTables)
                                    .nextToken(null)
                                    .build();
                        }
                    }
                    return response;
                });
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
        mockGlue.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames()
            throws Exception
    {
        List<Database> databases = new ArrayList<>();
        databases.add(Database.builder().name("db1").build());
        databases.add(Database.builder().name("db2").build());

        when(mockGlue.getDatabasesPaginator(nullable(GetDatabasesRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    GetDatabasesRequest request = (GetDatabasesRequest) invocationOnMock.getArguments()[0];
                    assertEquals(accountId, request.catalogId());
                    GetDatabasesIterable mockIterable = mock(GetDatabasesIterable.class);
                    GetDatabasesResponse response = GetDatabasesResponse.builder().databaseList(databases).build();
                    when(mockIterable.stream()).thenReturn(Collections.singletonList(response).stream());
                    return mockIterable;
                });

        ListSchemasRequest req = new ListSchemasRequest(IdentityUtil.fakeIdentity(), queryId, catalog);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertEquals(databases.stream().map(next -> next.name()).collect(Collectors.toList()),
                new ArrayList<>(res.getSchemas()));
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
        columns.add(Column.builder().name("col1").type("int").comment("comment").build());
        columns.add(Column.builder().name("col2").type("bigint").comment("comment").build());
        columns.add(Column.builder().name("col3").type("string").comment("comment").build());
        columns.add(Column.builder().name("col4").type("timestamp").comment("comment").build());
        columns.add(Column.builder().name("col5").type("date").comment("comment").build());
        columns.add(Column.builder().name("col6").type("timestamptz").comment("comment").build());
        columns.add(Column.builder().name("col7").type("timestamptz").comment("comment").build());
        columns.add(Column.builder().name("partition_col1").type("int").comment("comment").build());
        
        StorageDescriptor sd = StorageDescriptor.builder().columns(columns).build();
        Table returnTable = Table.builder().storageDescriptor(sd).name(table).parameters(expectedParams).build();

        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                        software.amazon.awssdk.services.glue.model.GetTableRequest request =
                            (software.amazon.awssdk.services.glue.model.GetTableRequest) invocationOnMock.getArguments()[0];

                    assertEquals(accountId, request.catalogId());
                    assertEquals(schema, request.databaseName());
                    assertEquals(table, request.name());

                    software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(returnTable)
                        .build();
                    return tableResponse;
                });

        GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table), Collections.emptyMap());
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
            StorageDescriptor storageDescriptor = StorageDescriptor.builder()
                        .location(String.format("arn:%s:dynamodb:us-east-1:012345678910:table/My-Table", partition))
                        .build();
            Table table = Table.builder()
                        .parameters(params)
                        .storageDescriptor(storageDescriptor)
                        .build();
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
        columns.add(Column.builder().name("col1").type("int").comment(" ").build());

        StorageDescriptor sd = StorageDescriptor.builder().columns(columns).build();
        Table resultTable = Table.builder().storageDescriptor(sd).parameters(expectedParams).build();

        when(mockGlue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) ->
                {
                    software.amazon.awssdk.services.glue.model.GetTableRequest request =
                            (software.amazon.awssdk.services.glue.model.GetTableRequest) invocationOnMock.getArguments()[0];

                    assertEquals(accountId, request.catalogId());
                    assertEquals(schema, request.databaseName());
                    assertEquals(table, request.name());

                    software.amazon.awssdk.services.glue.model.GetTableResponse response = software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                            .table(resultTable)
                            .build();
                    return response;
                });

        GetTableRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table), Collections.emptyMap());
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
        MetadataRequest req = new GetTableRequest(IdentityUtil.fakeIdentity(), queryId, catalog, new TableName(schema, table), Collections.emptyMap());
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
