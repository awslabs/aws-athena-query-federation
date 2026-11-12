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
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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
import org.mockito.junit.MockitoJUnitRunner;
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

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COLLECTION;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COMPONENT_TYPE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.DATABASE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.TRAVERSE;
import static org.junit.Assert.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneMetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandlerTest.class);
    private static final String GREMLIN_QPT_FUNCTION = "system.traverse";

    @Mock
    private GlueClient glue;

    private NeptuneMetadataHandler handler = null;

    private boolean enableTests = System.getenv("publishing") != null
            && System.getenv("publishing").equalsIgnoreCase("true");

    private BlockAllocatorImpl allocator;

    @Mock
    private NeptuneConnection neptuneConnection;
    private static final Map<String, String> DEFAULT_PARAMS = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        handler = new NeptuneMetadataHandler(glue,neptuneConnection,
                new LocalKeyFactory(), mock(SecretsManagerClient.class), mock(AthenaClient.class), "spill-bucket",
                "spill-prefix", DEFAULT_PARAMS);
        logger.info("setUpBefore - exit");
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void doListSchemaNames_WithValidRequest_ReturnsNonEmptySchemas() {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, "queryId", "default");

        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertFalse(res.getSchemas().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables_WithValidRequest_ReturnsNonEmptyTables() {

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
    public void doGetTable_WithValidRequest_ReturnsSchemaWithFields() throws Exception {

        logger.info("doGetTable - enter");

        Map<String, String> expectedParams = new HashMap<>();

        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col1").type("int").comment("comment").build());
        columns.add(Column.builder().name("col2").type("bigint").comment("comment").build());
        columns.add(Column.builder().name("col3").type("string").comment("comment").build());
        columns.add(Column.builder().name("col4").type("timestamp").comment("comment").build());
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

    @Test
    public void doGetQueryPassthroughSchema_withMapGremlinResult_returnsSchema() throws Exception
    {
        GraphTraversalSource graphTraversalSource = buildGremlinTestGraph();

        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

        GetTableRequest request = buildGremlinQptRequest(
                "g.V().project('name').by(values('name'))");

        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        assertNotNull(response.getSchema());
        assertFalse(response.getSchema().getFields().isEmpty());
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

    private GraphTraversalSource buildGremlinTestGraph()
    {
        try (TinkerGraph tinkerGraph = TinkerGraph.open()) {
            Vertex vertex = tinkerGraph.addVertex(T.label, "airport");
            vertex.property("name", "LAX");
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

    private void initHandlerForQueryPassthrough(String graphType, NeptuneConnection neptuneConn)
    {
        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, graphType);
        handler = new NeptuneMetadataHandler(glue, neptuneConn,
                new LocalKeyFactory(), mock(SecretsManagerClient.class), mock(AthenaClient.class), "spill-bucket",
                "spill-prefix", config);
    }

    @Test
    public void doGetQueryPassthroughSchema_WithRdfAndValidSparql_ReturnsValidSchema() throws Exception
    {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("s", "subject1");
        resultMap.put("p", "predicate1");
        resultMap.put("o", "object1");

        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        when(sparqlConnection.next()).thenReturn(resultMap);

        initHandlerForQueryPassthrough("rdf", sparqlConnection);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                createPassthroughRequest("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"));

        assertNotNull(res);
        assertEquals("table1", res.getTableName().getTableName());
        assertFalse(res.getSchema().getFields().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doGetQueryPassthroughSchema_WithInvalidGraphType_ThrowsIllegalArgumentException() throws Exception
    {
        initHandlerForQueryPassthrough("INVALID_TYPE", neptuneConnection);

        GetTableRequest req = createPassthroughRequest("g.V().hasLabel(\"airport\").valueMap()");
        handler.doGetQueryPassthroughSchema(allocator, req);
    }

    @Test
    public void doGetDataSourceCapabilities_WithValidRequest_ReturnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(IDENTITY, "queryId", DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getCapabilities());
    }

    @Test(expected = NullPointerException.class)
    public void doGetTable_WithNullGlue_ThrowsNullPointerException() throws Exception
    {
        NeptuneMetadataHandler nullGlueHandler = new NeptuneMetadataHandler(null, neptuneConnection,
                new LocalKeyFactory(), mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "spill-bucket", "spill-prefix", DEFAULT_PARAMS);

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default",
                new TableName("schema1", "table1"), Collections.emptyMap());
        nullGlueHandler.doGetTable(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_WithRdfAndInvalidSparql_ThrowsRuntimeException() throws Exception
    {
        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        doThrow(new RuntimeException("Invalid SPARQL query")).when(sparqlConnection).runQuery(anyString());

        initHandlerForQueryPassthrough("rdf", sparqlConnection);

        handler.doGetQueryPassthroughSchema(allocator, createPassthroughRequest("INVALID SPARQL QUERY"));
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndEmptyResponse_ThrowsRuntimeException() throws Exception
    {
        initHandlerForQueryPassthrough("propertygraph", neptuneConnection);

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);

        Client clientMock = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(clientMock);
        when(neptuneConnection.getTraversalSource(clientMock))
                .thenReturn(mock(GraphTraversalSource.class));

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            handler.doGetQueryPassthroughSchema(allocator,
                    createPassthroughRequest("g.V().hasLabel('nonexistent').valueMap().limit(1)"));
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndValidGremlin_ReturnsValidSchema() throws Exception
    {
        initHandlerForQueryPassthrough("propertygraph", neptuneConnection);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", "SFO");
        resultMap.put("city", "San Francisco");

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true, false);
        when(graphTraversalMock.next()).thenReturn(resultMap);

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                    createGremlinPassthroughRequest("g.V().hasLabel(\"airport\").valueMap().limit(1)"));

            assertNotNull(res);
            assertEquals("table1", res.getTableName().getTableName());
            assertFalse(res.getSchema().getFields().isEmpty());
            assertEquals(2, res.getSchema().getFields().size());
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndPartialColumns_ReturnsSchemaWithAvailableColumns() throws Exception
    {
        initHandlerForQueryPassthrough("propertygraph", neptuneConnection);

        // Create a result map with only one column, while Glue schema has two columns
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", "SFO");
        // Note: "city" column is missing from results

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true);
        when(graphTraversalMock.next()).thenReturn(resultMap);

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                    createGremlinPassthroughRequest("g.V().hasLabel(\"airport\").valueMap(\"code\").limit(1)"));

            assertNotNull(res);
            assertEquals("table1", res.getTableName().getTableName());
            assertFalse(res.getSchema().getFields().isEmpty());
            assertEquals(1, res.getSchema().getFields().size());
            assertEquals("code", res.getSchema().getFields().get(0).getName());
        }
    }

    private GetTableRequest createPassthroughRequest(String query)
    {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.QUERY, query);
        return new GetTableRequest(IDENTITY, "queryId", "catalog",
                new TableName("schema1", "table1"), passthroughArgs);
    }

    private GetTableRequest createGremlinPassthroughRequest(String query)
    {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.TRAVERSE");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.TRAVERSE, query);
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.COMPONENT_TYPE, "valueMap");
        return new GetTableRequest(IDENTITY, "queryId", "catalog",
                new TableName("schema1", "table1"), passthroughArgs);
    }
}
