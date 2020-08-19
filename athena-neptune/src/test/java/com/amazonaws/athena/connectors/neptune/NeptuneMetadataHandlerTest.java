/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
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
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.collections.CursorableLinkedList.Cursor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneMetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandlerTest.class);

    @Mock
    private AWSGlue glue;

    @Mock
    private GetTablesRequest glueReq = null;

    private NeptuneMetadataHandler handler = null;

    private boolean enableTests = System.getenv("publishing") != null
            && System.getenv("publishing").equalsIgnoreCase("true");

    private BlockAllocatorImpl allocator;

    @Mock
    private NeptuneConnection neptuneConnection;

    @Before
    public void setUp() throws Exception {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        handler = new NeptuneMetadataHandler(glue,neptuneConnection,
                new LocalKeyFactory(), mock(AWSSecretsManager.class), mock(AmazonAthena.class), "spill-bucket",
                "spill-prefix");
        logger.info("setUpBefore - exit");
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void doListSchemaNames() {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, "queryId", "default");

        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertFalse(res.getSchemas().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables() {

        logger.info("doListTables - enter");

        List<Table> tables = new ArrayList<Table>();
        Table table1 = new Table();
        table1.setName("table1");
        Table table2 = new Table();
        table2.setName("table2");
        Table table3 = new Table();
        table3.setName("table3");

        tables.add(table1);
        tables.add(table2);
        tables.add(table3);

        GetTablesResult tableResult = new GetTablesResult();
        tableResult.setTableList(tables);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, "queryId", "default", "default");
        when(glue.getTables(any(GetTablesRequest.class))).thenReturn(tableResult);

        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());
        assertFalse(res.getTables().isEmpty());
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable() throws Exception {
        if (!enableTests) {
            // We do this because until you complete the tutorial these tests will fail.
            // When you attempt to publis
            // using ../toos/publish.sh ... it will set the publishing flag and force these
            // tests. This is how we
            // avoid breaking the build but still have a useful tutorial. We are also
            // duplicateing this block
            // on purpose since this is a somewhat odd pattern.
            logger.info("doGetTable: Tests are disabled, to enable them set the 'publishing' environment variable "
                    + "using maven clean install -Dpublishing=true");
            return;
        }

        logger.info("doGetTable - enter");

        Table table = new Table();
        table.setName("table1");

        Map<String, String> expectedParams = new HashMap<>();
        expectedParams.put("sourceTable", table.getName());
        expectedParams.put("columnMapping", "col2=Col2,col3=Col3, col4=Col4");
        expectedParams.put("datetimeFormatMapping", "col2=someformat2, col1=someformat1 ");

        table.setParameters(expectedParams);

        List<Column> columns = new ArrayList<>();
        columns.add(new Column().withName("col1").withType("int").withComment("comment"));
        columns.add(new Column().withName("col2").withType("bigint").withComment("comment"));
        columns.add(new Column().withName("col3").withType("string").withComment("comment"));
        columns.add(new Column().withName("col4").withType("timestamp").withComment("comment"));
        columns.add(new Column().withName("col5").withType("date").withComment("comment"));
        columns.add(new Column().withName("col6").withType("timestamptz").withComment("comment"));
        columns.add(new Column().withName("col7").withType("timestamptz").withComment("comment"));

        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setColumns(columns);
        table.setStorageDescriptor(storageDescriptor);

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default", new TableName("schema1", "table1"));

        GetTableResult getTableResult = new GetTableResult();
        getTableResult.setTable(table);

        when(glue.getTable(any(com.amazonaws.services.glue.model.GetTableRequest.class))).thenReturn(getTableResult);

        GetTableResponse res = handler.doGetTable(allocator, req);

        assertTrue(res.getSchema().getFields().size() > 0);

        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

}
