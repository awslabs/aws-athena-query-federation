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
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COLLECTION;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COMPONENT_TYPE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.DATABASE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.TRAVERSE;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneMetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandlerTest.class);
    private static final String GREMLIN_QPT_FUNCTION = "system.traverse";

    @Mock
    private GlueClient glue;

    private NeptuneMetadataHandler handler = null;

    private BlockAllocatorImpl allocator;

    @Mock
    private NeptuneConnection neptuneConnection;

    @Before
    public void setUp() throws Exception {
        logger.info("setUp - enter");
        allocator = new BlockAllocatorImpl();
        handler = new NeptuneMetadataHandler(glue,neptuneConnection,
                new LocalKeyFactory(), mock(SecretsManagerClient.class), mock(AthenaClient.class), "spill-bucket",
                "spill-prefix", com.google.common.collect.ImmutableMap.of("glue_database_name", DEFAULT_SCHEMA));
        logger.info("setUp - exit");
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void doListSchemaNames_withValidRequest_returnsNonEmptySchemas() {
        logger.info("doListSchemaNames_withValidRequest_returnsNonEmptySchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, "queryId", "default");

        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemaNames_withValidRequest_returnsNonEmptySchemas - {}", res.getSchemas());
        assertEquals(1, res.getSchemas().size());
        assertTrue(res.getSchemas().contains(DEFAULT_SCHEMA));
        logger.info("doListSchemaNames_withValidRequest_returnsNonEmptySchemas - exit");
    }

    @Test
    public void doListTables_withValidRequest_returnsNonEmptyTables() {
        logger.info("doListTables_withValidRequest_returnsNonEmptyTables - enter");

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

        logger.info("doListTables_withValidRequest_returnsNonEmptyTables - {}", res.getTables());
        assertEquals(3, res.getTables().size());
        assertTrue(res.getTables().contains(new TableName("default", "table1")));
        assertTrue(res.getTables().contains(new TableName("default", "table2")));
        assertTrue(res.getTables().contains(new TableName("default", "table3")));
        logger.info("doListTables_withValidRequest_returnsNonEmptyTables - exit");
    }

    @Test
    public void doGetTable_withValidRequest_returnsSchemaWithFields() throws Exception {
        logger.info("doGetTable_withValidRequest_returnsSchemaWithFields - enter");

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

        assertEquals(7, res.getSchema().getFields().size());
        assertEquals("col1", res.getSchema().getFields().get(0).getName());
        assertEquals("col7", res.getSchema().getFields().get(6).getName());

        logger.info("doGetTable_withValidRequest_returnsSchemaWithFields - {}", res);
        logger.info("doGetTable_withValidRequest_returnsSchemaWithFields - exit");
    }

    @Test(expected = NullPointerException.class)
    public void doGetTable_withNullGlue_throwsNullPointerException() throws Exception
    {
        NeptuneMetadataHandler nullGlueHandler = new NeptuneMetadataHandler(
                null, neptuneConnection, new LocalKeyFactory(), mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "spill-bucket", "spill-prefix",
                com.google.common.collect.ImmutableMap.of("glue_database_name", DEFAULT_SCHEMA));

        GetTableRequest req = new GetTableRequest(
                IDENTITY, "queryId", "default", new TableName("schema1", "table1"), Collections.emptyMap());
        nullGlueHandler.doGetTable(allocator, req);
    }

    @Test
    public void doGetQueryPassthroughSchema_withPropertyGraphAndValidGremlin_returnsValidSchema() throws Exception
    {
        GraphTraversalSource graphTraversalSource = buildGremlinTestGraph();

        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

        GetTableRequest request = buildGremlinQptRequest("g.V().project('name').by(values('name'))");

        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        assertNotNull(response.getSchema());
        assertEquals(1, response.getSchema().getFields().size());
        assertEquals("name", response.getSchema().getFields().get(0).getName());
    }

    @Test
    public void doGetQueryPassthroughSchema_withPropertyGraphAndPartialColumns_returnsSchemaWithAvailableColumns()
            throws Exception
    {
        GraphTraversalSource graphTraversalSource = buildGremlinTestGraph();

        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

        GetTableResponse response = handler.doGetQueryPassthroughSchema(
                allocator, buildGremlinQptRequest("g.V().valueMap('name')"));

        assertEquals(1, response.getSchema().getFields().size());
        assertEquals("name", response.getSchema().getFields().get(0).getName());
    }

    @Test
    public void doGetQueryPassthroughSchema_withNonMapGremlinResult_throwsAthenaConnectorException() throws Exception
    {
        GraphTraversalSource graphTraversalSource = buildGremlinTestGraph();

        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

        GetTableRequest request = buildGremlinQptRequest("g.V().values('name')");

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertTrue(e.getMessage().contains("Unsupported gremlin query result shape"));
            assertTrue(e.getMessage().contains("Map"));
        }
    }

    @Test
    public void doGetDataSourceCapabilities_withValidRequest_returnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request =
                new GetDataSourceCapabilitiesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);

        GetDataSourceCapabilitiesResponse response =
                handler.doGetDataSourceCapabilities(allocator, request);

        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertNotNull(response.getCapabilities());
        assertFalse(response.getCapabilities().isEmpty());
    }

    @Test
    public void doGetQueryPassthroughSchema_withRdfAndValidSparql_returnsValidSchema() throws Exception
    {
        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        Map<String, Object> result = new HashMap<>();
        result.put("s", "subject");
        result.put("p", "predicate");
        result.put("o", "object");
        when(sparqlConnection.hasNext()).thenReturn(true);
        when(sparqlConnection.next()).thenReturn(result);
        initHandlerForQueryPassthrough("rdf", sparqlConnection);

        GetTableResponse response = handler.doGetQueryPassthroughSchema(
                allocator, buildSparqlQptRequest("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"));

        assertEquals(new TableName("testDb", "triples"), response.getTableName());
        assertEquals(3, response.getSchema().getFields().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doGetQueryPassthroughSchema_withInvalidGraphType_throwsIllegalArgumentException()
            throws Exception
    {
        initHandlerForQueryPassthrough("invalid", neptuneConnection);
        handler.doGetQueryPassthroughSchema(
                allocator, buildGremlinQptRequest("g.V().valueMap()"));
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_withRdfAndInvalidSparql_throwsRuntimeException()
            throws Exception
    {
        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        doThrow(new RuntimeException("Invalid SPARQL query"))
                .when(sparqlConnection).runQuery(anyString());
        initHandlerForQueryPassthrough("rdf", sparqlConnection);

        handler.doGetQueryPassthroughSchema(
                allocator, buildSparqlQptRequest("INVALID SPARQL QUERY"));
    }

    @Test(expected = NoSuchElementException.class)
    public void doGetQueryPassthroughSchema_withPropertyGraphAndEmptyResponse_throwsNoSuchElementException()
            throws Exception
    {
        initHandlerForQueryPassthrough("propertygraph", neptuneConnection);
        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(client)).thenReturn(mock(GraphTraversalSource.class));
        GraphTraversal<?, ?> traversal = mock(GraphTraversal.class);
        when(traversal.hasNext()).thenReturn(false);

        try (MockedConstruction<com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler> ignored =
                     mockConstruction(
                             com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(traversal))) {
            handler.doGetQueryPassthroughSchema(
                    allocator, buildGremlinQptRequest("g.V().hasLabel('missing').valueMap()"));
        }
    }

    private void initHandlerForQueryPassthrough(String graphType, NeptuneConnection connection)
    {
        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, graphType);
        handler = new NeptuneMetadataHandler(
                glue, connection, new LocalKeyFactory(), mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "spill-bucket", "spill-prefix", config);
    }

    private GraphTraversalSource buildGremlinTestGraph()
    {
        try (TinkerGraph tinkerGraph = TinkerGraph.open()) {
            Vertex vertex = tinkerGraph.addVertex(T.label, "airport");
            vertex.property("name", "LAX");
            vertex.property("city", "Los Angeles");
            return tinkerGraph.traversal();
        }
    }

    private GetTableRequest buildGremlinQptRequest(String traverse)
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(SCHEMA_FUNCTION_NAME, GREMLIN_QPT_FUNCTION);
        qptArguments.put(DATABASE, "testDb");
        qptArguments.put(COLLECTION, "airport");
        qptArguments.put(COMPONENT_TYPE, "vertex");
        qptArguments.put(TRAVERSE, traverse);

        return new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                new TableName("testDb", "airport"),
                qptArguments);
    }

    private GetTableRequest buildSparqlQptRequest(String query)
    {
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(SCHEMA_FUNCTION_NAME, "system.query");
        qptArguments.put(
                com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.DATABASE,
                "testDb");
        qptArguments.put(
                com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.COLLECTION,
                "triples");
        qptArguments.put(
                com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.QUERY,
                query);
        return new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                new TableName("testDb", "triples"),
                qptArguments);
    }

}
