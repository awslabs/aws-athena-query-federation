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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneMetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandlerTest.class);

    @Mock
    private GlueClient glue;

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
                new LocalKeyFactory(), mock(SecretsManagerClient.class), mock(AthenaClient.class), "spill-bucket",
                "spill-prefix", com.google.common.collect.ImmutableMap.of());
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
        Table table1 = Table.builder().name("table1").build();
        Table table2 = Table.builder().name("table2").build();
        Table table3 = Table.builder().name("table3").build();

        tables.add(table1);
        tables.add(table2);
        tables.add(table3);

        GetTablesResponse tableResponse = GetTablesResponse.builder().tableList(tables).build();

        ListTablesRequest req = new ListTablesRequest(IDENTITY, "queryId", "default",
                "default", null, UNLIMITED_PAGE_SIZE_VALUE);
        when(glue.getTables(nullable(GetTablesRequest.class))).thenReturn(tableResponse);

        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());
        assertFalse(res.getTables().isEmpty());
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable() throws Exception {

        logger.info("doGetTable - enter");

        Map<String, String> expectedParams = new HashMap<>();

        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col1").type("int").comment("comment").build());
        columns.add(Column.builder().name("col2").type("bigint").comment("comment").build());
        columns.add(Column.builder().name("col3").type("string").comment("comment").build());
        columns.add(Column.builder().name("col4").type("timestamp").comment("comm.build()ent").build());
        columns.add(Column.builder().name("col5").type("date").comment("comment").build());
        columns.add(Column.builder().name("col6").type("timestamptz").comment("comment").build());
        columns.add(Column.builder().name("col7").type("timestamptz").comment("comment").build());

        StorageDescriptor storageDescriptor = StorageDescriptor.builder().columns(columns).build();
        Table table = Table.builder()
                .name("table1")
                .parameters(expectedParams)
                .storageDescriptor(storageDescriptor)
                .build();

        expectedParams.put("sourceTable", table.name());
        expectedParams.put("columnMapping", "col2=Col2,col3=Col3, col4=Col4");
        expectedParams.put("datetimeFormatMapping", "col2=someformat2, col1=someformat1 ");

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default", new TableName("schema1", "table1"), Collections.emptyMap());

        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();

        when(glue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);

        GetTableResponse res = handler.doGetTable(allocator, req);

        assertTrue(res.getSchema().getFields().size() > 0);

        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

}
