/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COLLECTION;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.COMPONENT_TYPE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.DATABASE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough.TRAVERSE;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PropertyGraphHandlerTest
{
    private static final String VERTEX_TYPE = "vertex";
    private static final String VIEW_TYPE = "view";
    private static final String TEST_DB = "testDb";
    private static final String TEST_TABLE = "testTable";
    private static final String CUSTOM_LABEL = "customLabel";
    private static final String COUNT_FIELD = "count";
    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "name";
    private static final String GREMLIN_COUNT_QUERY = "g.V().count()";
    private static final String PERSON_VALUEMAP_TRAVERSE = "g.V().hasLabel('person').valueMap()";

    private GraphTraversalSource g;
    private PropertyGraphHandler handler;
    private BlockAllocatorImpl allocator;

    @Before
    public void setUp()
    {
        g = traversal().withEmbedded(TinkerFactory.createModern());
        handler = new PropertyGraphHandler(null);
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown() throws Exception
    {
        if (g != null) {
            g.getGraph().close();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    public void getResponseFromGremlinQuery_validValueMap_returnsTraversal() throws ScriptException
    {
        Object result = handler.getResponseFromGremlinQuery(g, "g.V().hasLabel('person').valueMap().limit(5)");
        assertNotNull(result);
        assertTrue(result instanceof GraphTraversal);
    }

    @Test
    public void getResponseFromGremlinQuery_validElementMap_returnsTraversal() throws ScriptException
    {
        Object result = handler.getResponseFromGremlinQuery(g, "g.V().hasLabel('person').elementMap().limit(5)");
        assertNotNull(result);
        assertTrue(result instanceof GraphTraversal);
    }

    @Test
    public void getResponseFromGremlinQuery_validProjectByValues_returnsTraversal() throws ScriptException
    {
        Object result = handler.getResponseFromGremlinQuery(g,
                "g.V().hasLabel('person').project('name').by(values('name')).limit(5)");
        assertNotNull(result);
        assertTrue(result instanceof GraphTraversal);
    }

    @Test(expected = ScriptException.class)
    public void getResponseFromGremlinQuery_nonGremlinThrow_throwsScriptException() throws ScriptException
    {
        handler.getResponseFromGremlinQuery(g,
                "throw new RuntimeException('INVALID_GREMLIN'); g.V().valueMap()");
    }

    @Test(expected = ScriptException.class)
    public void getResponseFromGremlinQuery_nonGremlinSystemCall_throwsScriptException() throws ScriptException
    {
        handler.getResponseFromGremlinQuery(g, "System.err.println('x'); g.V().valueMap()");
    }

    @Test(expected = AthenaConnectorException.class)
    public void executeQuery_withGremlinPassthroughMissingTraverse_throwsAthenaConnectorException()
            throws Exception
    {
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(mock(Client.class));
        when(connection.getTraversalSource(org.mockito.ArgumentMatchers.any(Client.class)))
                .thenReturn(mock(GraphTraversalSource.class));
        handler = new PropertyGraphHandler(connection);
        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        Constraints constraints = mock(Constraints.class);
        when(request.getConstraints()).thenReturn(constraints);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(Map.of(
                NeptuneGremlinQueryPassthrough.DATABASE, "database",
                NeptuneGremlinQueryPassthrough.COLLECTION, "airport",
                NeptuneGremlinQueryPassthrough.COMPONENT_TYPE, VERTEX_TYPE));

        handler.executeQuery(
                request,
                mock(QueryStatusChecker.class),
                mock(BlockSpiller.class),
                Collections.emptyMap());
    }

    @Test
    public void executeQuery_withEdgeType_writesRows() throws Exception
    {
        GraphTraversal graphTraversal = mock(GraphTraversal.class);
        Client client = mock(Client.class);
        GraphTraversalSource source = mock(GraphTraversalSource.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(source);
        when(source.E()).thenReturn(graphTraversal);
        when(graphTraversal.hasLabel(TEST_TABLE)).thenReturn(graphTraversal);
        when(graphTraversal.elementMap()).thenReturn(graphTraversal);
        when(graphTraversal.hasNext()).thenReturn(true, false);
        Map<String, Object> edgeData = new HashMap<>();
        edgeData.put(T.id.toString(), "e1");
        when(graphTraversal.next()).thenReturn(edgeData);

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, "edge")
                .addStringField(ID_FIELD)
                .build();
        BlockSpiller spiller = mockSpillerThatWrites(schema);

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, nonPassthroughConstraints()),
                runningQuery(),
                spiller,
                Collections.emptyMap());

        verify(spiller).writeRows(any());
        verify(graphTraversal).hasLabel(TEST_TABLE);
    }

    @Test
    public void executeQuery_withCustomGlabel_usesGlabelForHasLabel() throws Exception
    {
        GraphTraversal graphTraversal = mock(GraphTraversal.class);
        Client client = mock(Client.class);
        GraphTraversalSource source = mock(GraphTraversalSource.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(source);
        when(source.V()).thenReturn(graphTraversal);
        when(graphTraversal.hasLabel(CUSTOM_LABEL)).thenReturn(graphTraversal);
        when(graphTraversal.valueMap()).thenReturn(graphTraversal);
        when(graphTraversal.with(any())).thenReturn(graphTraversal);
        when(graphTraversal.hasNext()).thenReturn(false);

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VERTEX_TYPE)
                .addMetadata(Constants.SCHEMA_GLABEL, CUSTOM_LABEL)
                .addStringField(ID_FIELD)
                .build();

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, nonPassthroughConstraints()),
                runningQuery(),
                mock(BlockSpiller.class),
                Collections.emptyMap());

        verify(graphTraversal).hasLabel(CUSTOM_LABEL);
        verify(graphTraversal, never()).hasLabel(TEST_TABLE);
    }

    @Test
    public void executeQuery_withQueryPassthroughAndValidGremlin_writesTraversal() throws Exception
    {
        Client client = mock(Client.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(g);

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VERTEX_TYPE)
                .addStringField(NAME_FIELD)
                .build();
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, passthroughConstraints(gremlinPassthroughArgs(
                ))),
                queryStatusChecker,
                mock(BlockSpiller.class),
                Collections.emptyMap());

        verify(queryStatusChecker).isQueryRunning();
    }

    @Test
    public void executeQuery_withViewType_writesRows() throws Exception
    {
        Client client = mock(Client.class);
        ResultSet resultSet = mock(ResultSet.class);
        Result result = mock(Result.class);
        @SuppressWarnings("unchecked")
        Iterator<Result> iterator = mock(Iterator.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(mock(GraphTraversalSource.class));
        when(client.submit(GREMLIN_COUNT_QUERY)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(result);
        when(result.getObject()).thenReturn(Collections.singletonMap(COUNT_FIELD, 42L));

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE)
                .addMetadata(Constants.SCHEMA_QUERY, GREMLIN_COUNT_QUERY)
                .addBigIntField(COUNT_FIELD)
                .build();
        BlockSpiller spiller = mockSpillerThatWrites(schema);

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, nonPassthroughConstraints()),
                runningQuery(),
                spiller,
                Collections.emptyMap());

        verify(spiller).writeRows(any());
        verify(client).submit(GREMLIN_COUNT_QUERY);
    }

    @Test
    public void executeQuery_withViewTypeAndPassthrough_submitsTraverseQuery() throws Exception
    {
        Client client = mock(Client.class);
        ResultSet resultSet = mock(ResultSet.class);
        @SuppressWarnings("unchecked")
        Iterator<Result> iterator = mock(Iterator.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(g);
        when(client.submit(PERSON_VALUEMAP_TRAVERSE)).thenReturn(resultSet);
        when(resultSet.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE)
                .addStringField(NAME_FIELD)
                .build();

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, passthroughConstraints(gremlinPassthroughArgs(
                ))),
                runningQuery(),
                mock(BlockSpiller.class),
                Collections.emptyMap());

        verify(client).submit(PERSON_VALUEMAP_TRAVERSE);
    }

    @Test
    public void executeQuery_withQueryTermination_doesNotWriteRows() throws Exception
    {
        GraphTraversal graphTraversal = mock(GraphTraversal.class);
        Client client = mock(Client.class);
        GraphTraversalSource source = mock(GraphTraversalSource.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(source);
        when(source.V()).thenReturn(graphTraversal);
        when(graphTraversal.hasLabel(anyString())).thenReturn(graphTraversal);
        when(graphTraversal.valueMap()).thenReturn(graphTraversal);
        when(graphTraversal.with(any())).thenReturn(graphTraversal);
        when(graphTraversal.hasNext()).thenReturn(true);

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VERTEX_TYPE)
                .addStringField(ID_FIELD)
                .build();
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);
        BlockSpiller spiller = mock(BlockSpiller.class);

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, nonPassthroughConstraints()),
                queryStatusChecker,
                spiller,
                Collections.emptyMap());

        verify(spiller, never()).writeRows(any());
    }

    @Test(expected = NullPointerException.class)
    public void executeQuery_withViewTypeAndMissingQuery_throwsNullPointerException() throws Exception
    {
        Client client = mock(Client.class);
        NeptuneConnection connection = mock(NeptuneConnection.class);
        when(connection.getNeptuneClientConnection()).thenReturn(client);
        when(connection.getTraversalSource(client)).thenReturn(mock(GraphTraversalSource.class));

        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE)
                .addBigIntField(COUNT_FIELD)
                .build();

        handler = new PropertyGraphHandler(connection);
        handler.executeQuery(
                mockRequest(schema, nonPassthroughConstraints()),
                runningQuery(),
                mock(BlockSpiller.class),
                Collections.emptyMap());
    }

    private ReadRecordsRequest mockRequest(Schema schema, Constraints constraints)
    {
        ReadRecordsRequest request = mock(ReadRecordsRequest.class);
        when(request.getSchema()).thenReturn(schema);
        when(request.getTableName()).thenReturn(new TableName(TEST_DB, TEST_TABLE));
        when(request.getConstraints()).thenReturn(constraints);
        return request;
    }

    private Constraints nonPassthroughConstraints()
    {
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(false);
        return constraints;
    }

    private Constraints passthroughConstraints(Map<String, String> arguments)
    {
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(arguments);
        return constraints;
    }

    private Map<String, String> gremlinPassthroughArgs()
    {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(DATABASE, TEST_DB);
        arguments.put(COLLECTION, "person");
        arguments.put(COMPONENT_TYPE, VERTEX_TYPE);
        arguments.put(TRAVERSE, PropertyGraphHandlerTest.PERSON_VALUEMAP_TRAVERSE);
        arguments.put(SCHEMA_FUNCTION_NAME, "system.traverse");
        return arguments;
    }

    private QueryStatusChecker runningQuery()
    {
        QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        return queryStatusChecker;
    }

    private BlockSpiller mockSpillerThatWrites(Schema schema)
    {
        BlockSpiller spiller = mock(BlockSpiller.class);
        Block block = allocator.createBlock(schema);
        doAnswer(invocation -> {
            BlockSpiller.RowWriter writer = invocation.getArgument(0);
            writer.writeRows(block, 0);
            return null;
        }).when(spiller).writeRows(any());
        return spiller;
    }
}
