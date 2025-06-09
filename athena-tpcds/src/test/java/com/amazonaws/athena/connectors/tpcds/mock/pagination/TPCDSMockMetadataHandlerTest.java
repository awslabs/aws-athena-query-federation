/*-
 * #%L
 * athena-tpcds
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
package com.amazonaws.athena.connectors.tpcds.mock.pagination;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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
import com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.tpcds.TPCDSMetadataHandler.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class TPCDSMockMetadataHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TPCDSMockMetadataHandlerTest.class);

    private FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    private TPCDSMetadataHandler handler;
    private BlockAllocator allocator;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        handler = new TPCDSMockMetadataHandler(new LocalKeyFactory(), mockSecretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
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
        logger.info("doListSchemas - enter");

        ListSchemasRequest req = new ListSchemasRequest(identity, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertTrue(res.getSchemas().size() == 150);
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTablesNoPagination()
    {
        logger.info("doListTables - enter");

        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        assertTrue(res.getTables().contains(new TableName("tpcds1", "customer")));
        assertTrue(res.getTables().contains(new TableName("tpcds1", "mock_table_1")));
        assertTrue(res.getTables().size() == 125);

        logger.info("doListTables - exit");
    }

    @Test
    public void doListTablesWithPagination()
    {
        logger.info("doListTables - enter");
        // iteration 1, [0,10)
        ListTablesRequest req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", null, 10);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());
        assertTrue(res.getTables().size() == 10);
        assertTrue(res.getNextToken() != null);
        assertEquals(Integer.toString(10), res.getNextToken());

        // iteration 2, [10,30)
        req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", Integer.toString(10), 30);
        res = handler.doListTables(allocator, req);
        assertTrue(res.getTables().size() == 30);
        assertTrue(res.getNextToken() != null);
        assertEquals(Integer.toString(40), res.getNextToken());

        // iteration 3 [30, 120)
        req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", Integer.toString(30), 90);
        res = handler.doListTables(allocator, req);
        assertTrue(res.getTables().size() == 90);
        assertTrue(res.getNextToken() != null);
        assertEquals(Integer.toString(120), res.getNextToken());

        // iteration 4 [120, ... infinity)
        req = new ListTablesRequest(identity, "queryId", "default",
                "tpcds1", Integer.toString(120), 10);
        res = handler.doListTables(allocator, req);
        assertTrue(res.getTables().size() == 5);
        assertNull(res.getNextToken());

        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable()
    {
        logger.info("doGetTable - enter");
        String expectedSchema = "tpcds1";

        GetTableRequest req = new GetTableRequest(identity,
                "queryId",
                "default",
                new TableName(expectedSchema, "customer"), Collections.emptyMap());

        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", res.getTableName(), res.getSchema());

        assertEquals(new TableName(expectedSchema, "customer"), res.getTableName());
        assertTrue(res.getSchema() != null);

        logger.info("doGetTable - exit");
    }

    @Test
    public void doGetTableOnMockTable()
    {
        logger.info("doGetTableMockTable - enter");
        String expectedSchema = "tpcds1";

        GetTableRequest req = new GetTableRequest(identity,
                "queryId",
                "default",
                new TableName(expectedSchema, "mock_table_1"), Collections.emptyMap());

        GetTableResponse mock_table_res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {} {}", mock_table_res.getTableName(), mock_table_res.getSchema());

        assertEquals(new TableName(expectedSchema, "mock_table_1"), mock_table_res.getTableName());

        req = new GetTableRequest(identity,
                "queryId",
                "default",
                new TableName(expectedSchema, "mock_table_1"), Collections.emptyMap());

        GetTableResponse customer_table = handler.doGetTable(allocator, req);

        assertEquals(customer_table.getSchema(), mock_table_res.getSchema());

        logger.info("doGetTableMockTable - exit");
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        logger.info("doGetTableLayout - enter");

        Map<String, ValueSet> constraintsMap = new HashMap<>();

        Schema schema = SchemaBuilder.newBuilder().build();

        GetTableLayoutRequest req = new GetTableLayoutRequest(identity,
                "queryId",
                "default",
                new TableName("tpcds1", "customer"),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout - {}", res.getPartitions());

        assertTrue(res.getPartitions().getRowCount() == 1);

        logger.info("doGetTableLayout - exit");
    }
}
