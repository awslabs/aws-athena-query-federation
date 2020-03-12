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
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("id", "principal", "account");
    private String catalog = "default";
    private HbaseMetadataHandler handler;
    private BlockAllocator allocator;

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
        handler = new HbaseMetadataHandler(awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                "spillBucket",
                "spillPrefix");

        when(mockConnFactory.getOrCreateConn(anyString())).thenReturn(mockClient);

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
            throws IOException
    {
        logger.info("doListSchemaNames: enter");

        NamespaceDescriptor[] schemaNames = {NamespaceDescriptor.create("schema1").build(),
                NamespaceDescriptor.create("schema2").build(),
                NamespaceDescriptor.create("schema3").build()};

        when(mockClient.listNamespaceDescriptors()).thenReturn(schemaNames);

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
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
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(anyObject(), any(Scan.class), anyObject())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        GetTableRequest req = new GetTableRequest(identity, "queryId", catalog, new TableName(schema, table));
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        Schema expectedSchema = TestUtils.makeSchema()
                .addField(HbaseSchemaUtils.ROW_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .build();

        assertEquals(expectedSchema.getFields().size(), res.getSchema().getFields().size());
        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName("schema1", "table1"),
                new Constraints(new HashMap<>()),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

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
            throws IOException
    {
        logger.info("doGetSplits: enter");

        List<HRegionInfo> regionServers = new ArrayList<>();
        regionServers.add(TestUtils.makeRegion(1, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(2, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(3, "schema1", "table1"));
        regionServers.add(TestUtils.makeRegion(4, "schema1", "table1"));

        when(mockClient.getTableRegions(any())).thenReturn(regionServers);
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

        assertTrue("Continuation criteria violated", response.getSplits().size() == 4);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);

        logger.info("doGetSplits: exit");
    }
}
