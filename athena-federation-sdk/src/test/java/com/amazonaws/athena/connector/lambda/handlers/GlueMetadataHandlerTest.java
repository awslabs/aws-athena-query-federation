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
import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.proto.domain.TableName;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.proto.metadata.GetDataSourceCapabilitiesResponse;
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
import com.amazonaws.athena.connector.lambda.security.IdentityUtil;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufMessageConverter;
import com.amazonaws.athena.connector.lambda.serde.protobuf.ProtobufSerDe;
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
            .add(new Table().withName("table3"))
            .add(new Table().withName("table2"))
            .add(new Table().withName("table5"))
            .add(new Table().withName("table4"))
            .add(new Table().withName("table1"))
            .build();

    // The following response is expected be returned from doListTables when the pagination pageSize is greater than
    // the number of tables in the unPaginatedTables list (or pageSize has UNLIMITED_PAGE_SIZE_VALUE).
    private final ListTablesResponse fullListResponse = ListTablesResponse.newBuilder()
        .setCatalogName(catalog)
        .addAllTables(
            new ImmutableList.Builder<TableName>()
                    .add(TableName.newBuilder().setSchemaName(schema).setTableName("table1").build())
                    .add(TableName.newBuilder().setSchemaName(schema).setTableName("table2").build())
                    .add(TableName.newBuilder().setSchemaName(schema).setTableName("table3").build())
                    .add(TableName.newBuilder().setSchemaName(schema).setTableName("table4").build())
                    .add(TableName.newBuilder().setSchemaName(schema).setTableName("table5").build())
                    .build()
            .stream()
            .collect(Collectors.toList())
        )
        .build();
    
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
                "spill-prefix",
                new HashMap<>())
        {
            @Override
            public GetTableLayoutResponse doGetTableLayout(BlockAllocator blockAllocator, GetTableLayoutRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getPartitions(BlockAllocator allocator, BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
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
                    String nextToken = request.getNextToken();
                    int pageSize = request.getMaxResults() == null ? ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE : request.getMaxResults();
                    assertEquals(accountId, request.getCatalogId());
                    assertEquals(schema, request.getDatabaseName());
                    GetTablesResult mockResult = mock(GetTablesResult.class);
                    if (pageSize == ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE) {
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

        when(mockGlue.getDatabases(nullable(GetDatabasesRequest.class)))
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

        ListSchemasRequest req = ListSchemasRequest.newBuilder()
                .setQueryId(queryId)
                .setCatalogName(catalog)
                .setIdentity(IdentityUtil.fakeIdentity())
                .build();
        
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemasList());

        assertEquals(databases.stream().map(next -> next.getName()).collect(Collectors.toList()),
                new ArrayList<>(res.getSchemasList()));

        verify(mockGlue, times(2)).getDatabases(nullable(GetDatabasesRequest.class));
    }

    @Test
    public void doListTablesWithUnlimitedPageSize()
            throws Exception
    {
        ListTablesRequest req = ListTablesRequest.newBuilder()
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setPageSize(ProtobufSerDe.UNLIMITED_PAGE_SIZE_VALUE)
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();
        
        logger.info("Request - {}", req);
        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEqualsListTablesResponse(fullListResponse, actualResponse);
    }

    @Test
    public void doListTablesWithLargePageSize()
            throws Exception
    {
        ListTablesRequest req = ListTablesRequest.newBuilder()
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setPageSize(GET_TABLES_REQUEST_MAX_RESULTS + 50)
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();

        logger.info("Request - {}", req);
        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEqualsListTablesResponse(fullListResponse, actualResponse);
    }

    @Test
    public void doListTablesWithPagination()
            throws Exception
    {
        logger.info("First paginated request");
        ListTablesRequest req = ListTablesRequest.newBuilder()
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setPageSize(3)
            .setIdentity(IdentityUtil.fakeIdentity())
            .build();

        logger.info("Request - {}", req);
        ListTablesResponse expectedResponse = ListTablesResponse.newBuilder()
            .setCatalogName(req.getCatalogName())
            .setNextToken("table4")
            .addAllTables(
                new ImmutableList.Builder<TableName>()
                    .add(TableName.newBuilder().setSchemaName(req.getSchemaName()).setTableName("table1").build())
                    .add(TableName.newBuilder().setSchemaName(req.getSchemaName()).setTableName("table2").build())
                    .add(TableName.newBuilder().setSchemaName(req.getSchemaName()).setTableName("table3").build())
                    .build()
                .stream()
                .collect(Collectors.toList())
            )
            .build();

        ListTablesResponse actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEqualsListTablesResponse(expectedResponse, actualResponse);

        logger.info("Second paginated request");
        req = ListTablesRequest.newBuilder()
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setPageSize(3)
            .setIdentity(IdentityUtil.fakeIdentity())
            .setNextToken(expectedResponse.getNextToken())
            .build();

        logger.info("Request - {}", req);
        expectedResponse = ListTablesResponse.newBuilder()
            .setCatalogName(req.getCatalogName())
            .addAllTables(
                new ImmutableList.Builder<TableName>()
                    .add(TableName.newBuilder().setSchemaName(req.getSchemaName()).setTableName("table4").build())
                    .add(TableName.newBuilder().setSchemaName(req.getSchemaName()).setTableName("table5").build())
                    .build()
                .stream()
                .collect(Collectors.toList())
            )
            .build();
        
        actualResponse = handler.doListTables(allocator, req);
        logger.info("Response - {}", actualResponse);
        assertEqualsListTablesResponse(expectedResponse, actualResponse);
    }

    private void assertEqualsListTablesResponse(ListTablesResponse expected, ListTablesResponse actual)
    {
        // there was a bug in these tests before - the ExampleMetadataHandler doesn't actually sort the tables if it has no pagination,
        // but the tests implied they were supposed to by comparing the objects. However, the equals method defined in the old Response class
        // just checked if the two lists had all the same values (unordered). Because the equals method is now more refined for the generated
        // protobuf class, we have to manually do the same checks.
        assertTrue(CollectionsUtils.equals(expected.getTablesList(), actual.getTablesList()));
        assertEquals(expected.getCatalogName(), actual.getCatalogName());
        assertEquals(expected.getNextToken(), actual.getNextToken());
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

        Mockito.lenient().when(mockTable.getName()).thenReturn(table);
        when(mockTable.getStorageDescriptor()).thenReturn(mockSd);
        when(mockTable.getParameters()).thenReturn(expectedParams);
        when(mockSd.getColumns()).thenReturn(columns);

        when(mockGlue.getTable(nullable(com.amazonaws.services.glue.model.GetTableRequest.class)))
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

        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(IdentityUtil.fakeIdentity())
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setTableName(
                com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                    .setTableName(table)
                    .setSchemaName(schema)
                    .build()
            ).build();
        
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res);

        Schema arrowSchema = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema());

        assertTrue(arrowSchema.getFields().size() == 8);
        assertTrue(arrowSchema.getCustomMetadata().size() > 0);
        assertTrue(arrowSchema.getCustomMetadata().containsKey(DATETIME_FORMAT_MAPPING_PROPERTY));
        assertEquals(arrowSchema.getCustomMetadata().get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), "Col2=someformat2,col1=someformat1");
        assertEquals(sourceTable, getSourceTableName(arrowSchema));

        //Verify column name mapping works
        assertNotNull(arrowSchema.findField("partition_col1"));
        assertNotNull(arrowSchema.findField("col1"));
        assertNotNull(arrowSchema.findField("Col2"));
        assertNotNull(arrowSchema.findField("Col3"));
        assertNotNull(arrowSchema.findField("Col4"));
        assertNotNull(arrowSchema.findField("col5"));
        assertNotNull(arrowSchema.findField("col6"));
        assertNotNull(arrowSchema.findField("col7"));

        //Verify types
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("partition_col1").getType()).equals(Types.MinorType.INT));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("col1").getType()).equals(Types.MinorType.INT));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("Col2").getType()).equals(Types.MinorType.BIGINT));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("Col3").getType()).equals(Types.MinorType.VARCHAR));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("Col4").getType()).equals(Types.MinorType.DATEMILLI));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("col5").getType()).equals(Types.MinorType.DATEDAY));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("col6").getType()).equals(Types.MinorType.TIMESTAMPMILLITZ));
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("col7").getType()).equals(Types.MinorType.TIMESTAMPMILLITZ));
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

        Mockito.lenient().when(mockTable.getName()).thenReturn(table);
        when(mockTable.getStorageDescriptor()).thenReturn(mockSd);
        when(mockTable.getParameters()).thenReturn(expectedParams);
        when(mockSd.getColumns()).thenReturn(columns);

        when(mockGlue.getTable(nullable(com.amazonaws.services.glue.model.GetTableRequest.class)))
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

        GetTableRequest req = GetTableRequest.newBuilder()
            .setIdentity(IdentityUtil.fakeIdentity())
            .setQueryId(queryId)
            .setCatalogName(catalog)
            .setTableName(
                com.amazonaws.athena.connector.lambda.proto.domain.TableName.newBuilder()
                    .setTableName(table)
                    .setSchemaName(schema)
                    .build()
            ).build();
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res);

        Schema arrowSchema = ProtobufMessageConverter.fromProtoSchema(allocator, res.getSchema());

        //Verify column name mapping works
        assertNotNull(arrowSchema.findField("col1"));

        //Verify types
        assertTrue(Types.getMinorTypeForArrowType(arrowSchema.findField("col1").getType()).equals(Types.MinorType.INT));
    }

    @Test
    public void testGetCatalog() {
        // Catalog should be the account from the request
        com.amazonaws.athena.connector.lambda.proto.security.FederatedIdentity identity = IdentityUtil.fakeIdentity();
        String catalog = handler.getCatalog(identity);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);

        // Catalog should be the account from the lambda context's function arn
        handler.configOptions.put(MetadataHandler.FUNCTION_ARN_CONFIG_KEY, "arn:aws:lambda:us-east-1:012345678912:function:athena-123");
        catalog = handler.getCatalog(identity);
        assertEquals("012345678912", catalog);

        // Catalog should be the account from the request since function arn is invalid
        handler.configOptions.put(MetadataHandler.FUNCTION_ARN_CONFIG_KEY, "arn:aws:lambda:us-east-1:012345678912:function:");
        catalog = handler.getCatalog(identity);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);

        // Catalog should be the account from the request since function arn is null
        handler.configOptions.put(MetadataHandler.FUNCTION_ARN_CONFIG_KEY, null);
        catalog = handler.getCatalog(identity);
        assertEquals(IdentityUtil.fakeIdentity().getAccount(), catalog);
    }
}
