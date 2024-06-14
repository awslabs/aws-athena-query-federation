/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.HbaseConnectionFactory;
import com.amazonaws.athena.connectors.hbase.connection.ResultProcessor;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseMetadataHandlerTest.class);

    private HbaseMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private HBaseConnection mockClient;

    @Mock
    private HbaseConnectionFactory mockConnFactory;

    @Mock
    private AWSGlue awsGlue;

    @Mock
    private AWSSecretsManager secretsManager;

    @Mock
    private AmazonAthena athena;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());
        handler = new HbaseMetadataHandler(awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                "spillBucket",
                "spillPrefix",
                com.google.common.collect.ImmutableMap.of());

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

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
            throws IOException
    {
        NamespaceDescriptor[] schemaNames = {NamespaceDescriptor.create("schema1").build(),
                NamespaceDescriptor.create("schema2").build(),
                NamespaceDescriptor.create("schema3").build()};

        when(mockClient.listNamespaceDescriptors()).thenReturn(schemaNames);

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());
        Set<String> expectedSchemaName = new HashSet<>();
        expectedSchemaName.add("schema1");
        expectedSchemaName.add("schema2");
        expectedSchemaName.add("schema3");
        assertEquals(expectedSchemaName, new HashSet<>(res.getSchemas()));

        logger.info("doListSchemaNames: exit");
    }

    @Test
    public void doListTables()
            throws IOException
    {
        logger.info("doListTables - enter");

        String schema = "schema1";

        org.apache.hadoop.hbase.TableName[] tables = {
                org.apache.hadoop.hbase.TableName.valueOf("schema1", "table1"),
                org.apache.hadoop.hbase.TableName.valueOf("schema1", "table2"),
                org.apache.hadoop.hbase.TableName.valueOf("schema1", "table3")
        };

        Set<String> tableNames = new HashSet<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        when(mockClient.listTableNamesByNamespace(eq(schema))).thenReturn(tables);
        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        for (TableName next : res.getTables()) {
            assertEquals(schema, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());
    }

    /**
     * TODO: Add more types.
     */
    @Test
    public void doGetTable()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        Schema expectedSchema = TestUtils.makeSchema()
                .addField(HbaseSchemaUtils.ROW_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .build();

        assertEquals(expectedSchema.getFields().size(), res.getSchema().getFields().size());
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest req = new GetTableLayoutRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                SchemaBuilder.newBuilder().build(),
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
            throws IOException
    {
        List<HRegionInfo> regionServers = new ArrayList<>();
        regionServers.add(TestUtils.makeRegion(1, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(2, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(3, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(4, "schema1", "table1"));

        when(mockClient.getTableRegions(any())).thenReturn(regionServers);
        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                partitionCols,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == 4);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);
    }
}
