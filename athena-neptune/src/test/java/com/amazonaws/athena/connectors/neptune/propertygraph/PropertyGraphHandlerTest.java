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
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PropertyGraphHandlerTest
{
    private GraphTraversalSource g;
    private static final String VERTEX_TYPE = "VERTEX";
    private static final String EDGE_TYPE = "EDGE";
    private static final String VIEW_TYPE = "VIEW";
    private static final String TEST_DB = "testDb";
    private static final String TEST_TABLE = "testTable";
    private static final String PERSON_LABEL = "person";
    private static final String KNOWS_LABEL = "knows";
    private static final String CUSTOM_LABEL = "customLabel";
    private static final String COUNT_FIELD = "count";
    private static final String ID_FIELD = "id";
    private static final String LABEL_FIELD = "label";
    private static final String VERTEX_ID = "1";
    private static final String EDGE_ID = "1";
    private static final String GREMLIN_COUNT_QUERY = "g.V().count()";
    private static final String GREMLIN_QUERY = "g.V().count()";
    private static final String VALUE_MAP_TYPE = "valueMap";
    private static final String SYSTEM_TRAVERSE = "SYSTEM.TRAVERSE";
    private static final String SYSTEM_QUERY = "SYSTEM.QUERY";
    private static final String INVALID_QUERY = "invalid.query()";
    private static final String PERSON_VALUEMAP_TRAVERSE = "g.V().hasLabel('person').valueMap()";

    @Mock
    private NeptuneConnection neptuneConnection;
    @Mock
    private Client client;
    @Mock
    private GraphTraversalSource graphTraversalSource;
    @Mock
    private GraphTraversal graphTraversal;
    @Mock
    private BlockSpiller spiller;
    @Mock
    private QueryStatusChecker queryStatusChecker;
    @Mock
    private ReadRecordsRequest recordsRequest;
    @Mock
    private Block block;
    @Mock
    private ResultSet resultSet;
    private PropertyGraphHandler handler;
    private Schema schema;
    private Map<String, String> customMetadata;
    private AutoCloseable mocks;

    @Before
    public void setUp()
    {
        mocks = MockitoAnnotations.openMocks(this);
        g = traversal().withEmbedded(TinkerFactory.createModern());
        handler = new PropertyGraphHandler(neptuneConnection);

        // Setup common mocks
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(any())).thenReturn(graphTraversalSource);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        when(graphTraversalSource.V()).thenReturn(graphTraversal);
        when(graphTraversalSource.E()).thenReturn(graphTraversal);
        when(graphTraversal.hasLabel(anyString())).thenReturn(graphTraversal);
        when(graphTraversal.valueMap()).thenReturn(graphTraversal);
        when(graphTraversal.elementMap()).thenReturn(graphTraversal);
        when(graphTraversal.with(any())).thenReturn(graphTraversal);

        // Setup schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(ID_FIELD, FieldType.nullable(new ArrowType.Utf8()), null));
        fields.add(new Field(LABEL_FIELD, FieldType.nullable(new ArrowType.Utf8()), null));

        customMetadata = new HashMap<>();
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, VERTEX_TYPE); // Set default type
        schema = new Schema(fields, customMetadata);

        when(recordsRequest.getSchema()).thenReturn(schema);
        when(recordsRequest.getTableName()).thenReturn(new TableName(TEST_DB, TEST_TABLE));

        // Setup constraints with empty maps and lists
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), 0, Collections.emptyMap(), null);
        when(recordsRequest.getConstraints()).thenReturn(constraints);

        // Setup spiller mock to handle writeRows
        doAnswer((Answer<Void>) invocation -> {
            BlockSpiller.RowWriter rowWriter = (BlockSpiller.RowWriter) invocation.getArguments()[0];
            rowWriter.writeRows(block, 1);
            return null;
        }).when(spiller).writeRows(any());
    }

    @After
    public void tearDown() throws Exception
    {
        if (g != null) {
            g.getGraph().close();
        }
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void executeQuery_WithVertexType_ProcessesVertexData() throws Exception
    {
        // Setup vertex specific mocks
        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), VERTEX_ID);
        vertexData.put(T.label.toString(), PERSON_LABEL);
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    public void executeQuery_WithEdgeType_ProcessesEdgeData() throws Exception
    {
        // Setup edge specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, EDGE_TYPE);
        Schema edgeSchema = new Schema(schema.getFields(), new HashMap<>(customMetadata));
        when(recordsRequest.getSchema()).thenReturn(edgeSchema);
        Map<String, Object> edgeData = new HashMap<>();
        edgeData.put(T.id.toString(), EDGE_ID);
        edgeData.put(T.label.toString(), KNOWS_LABEL);
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(edgeData);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    public void executeQuery_WithViewType_ProcessesViewData() throws Exception
    {
        // Setup view specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE);
        customMetadata.put(Constants.SCHEMA_QUERY, GREMLIN_COUNT_QUERY);

        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COUNT_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);

        Result mockResult = mock(Result.class);
        when(mockResult.getObject()).thenReturn(42L);

        @SuppressWarnings("unchecked")
        Iterator<Result> resultIterator = mock(Iterator.class);
        when(resultIterator.hasNext()).thenReturn(true, false);
        when(resultIterator.next()).thenReturn(mockResult);

        when(resultSet.iterator()).thenReturn(resultIterator);
        when(client.submit(anyString())).thenReturn(resultSet);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(client, times(1)).submit(anyString());
    }

    @Test
    public void executeQuery_WithViewTypeAndPassthrough_ProcessesPassthroughData() throws Exception
    {
        // Setup view specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE);

        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COUNT_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);

        mockQueryPassThroughOnRecordsRequest(gremlinPassthroughArgs());

        Result mockResult = mock(Result.class);
        when(mockResult.getObject()).thenReturn(42L);

        @SuppressWarnings("unchecked")
        Iterator<Result> resultIterator = mock(Iterator.class);
        when(resultIterator.hasNext()).thenReturn(true, false);
        when(resultIterator.next()).thenReturn(mockResult);

        when(resultSet.iterator()).thenReturn(resultIterator);
        when(client.submit(anyString())).thenReturn(resultSet);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(client, times(1)).submit(anyString());
    }

    @Test
    public void executeQuery_WithQueryPassthrough_ProcessesPassthroughQuery() throws Exception
    {
        mockQueryPassThroughOnRecordsRequest(gremlinPassthroughArgs());

        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), VERTEX_ID);
        vertexData.put(T.label.toString(), PERSON_LABEL);
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    public void executeQuery_WithCustomLabel_ProcessesCustomLabelData() throws Exception
    {
        // Setup glabel specific mocks
        customMetadata.put(Constants.SCHEMA_GLABEL, CUSTOM_LABEL);
        when(recordsRequest.getTableName()).thenReturn(new TableName(TEST_DB, CUSTOM_LABEL));

        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), VERTEX_ID);
        vertexData.put(T.label.toString(), CUSTOM_LABEL);
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
        verify(graphTraversal).hasLabel(CUSTOM_LABEL);
    }

    @Test
    public void executeQuery_WithQueryTermination_DoesNotProcessData() throws Exception
    {
        // Setup termination test
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);

        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());

        // Verify
        verify(spiller, never()).writeRows(any());
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
    
    @Test
    public void getResponseFromGremlinQuery_WithValidQuery_ExecutesQuery() throws Exception
    {
        when(graphTraversalSource.V()).thenReturn(graphTraversal);
        when(graphTraversal.count()).thenReturn(graphTraversal);
        when(graphTraversal.next()).thenReturn(42L);
        
        handler.getResponseFromGremlinQuery(graphTraversalSource, GREMLIN_QUERY);
        
        verify(graphTraversalSource).V();
    }
    
    @Test
    public void getResponseFromGremlinQuery_WithInvalidQuery_ThrowsScriptException() throws Exception
    {
        assertThrows(ScriptException.class, () -> handler.getResponseFromGremlinQuery(graphTraversalSource, INVALID_QUERY));
    }
    
    @Test
    public void executeQuery_WithViewTypeAndInvalidSchema_ThrowsRuntimeException() throws Exception
    {
        // Setup view specific mocks with invalid schema (no query)
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, VIEW_TYPE);
        
        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COUNT_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);
        
        // Execute test and verify exception
        assertThrows(RuntimeException.class, () -> handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>()));
    }
    
    @Test
    public void executeQuery_WithQueryPassthroughAndInvalidQuery_ThrowsRuntimeException() throws Exception
    {
        // Passthrough enabled but SYSTEM.QUERY without traverse — handler should throw
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(NeptuneGremlinQueryPassthrough.DATABASE, TEST_DB);
        qptArgs.put(NeptuneGremlinQueryPassthrough.COLLECTION, PERSON_LABEL);
        qptArgs.put(SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        mockQueryPassThroughOnRecordsRequest(qptArgs);
        
        // Execute test and verify exception
        assertThrows(RuntimeException.class, () -> handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>()));
    }
    
    private static Map<String, String> gremlinPassthroughArgs()
    {
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(NeptuneGremlinQueryPassthrough.DATABASE, TEST_DB);
        qptArgs.put(NeptuneGremlinQueryPassthrough.COLLECTION, PERSON_LABEL);
        qptArgs.put(NeptuneGremlinQueryPassthrough.TRAVERSE, PERSON_VALUEMAP_TRAVERSE);
        qptArgs.put(NeptuneGremlinQueryPassthrough.COMPONENT_TYPE, VALUE_MAP_TYPE);
        qptArgs.put(SCHEMA_FUNCTION_NAME, SYSTEM_TRAVERSE);
        return qptArgs;
    }
    
    private void mockQueryPassThroughOnRecordsRequest(Map<String, String> qptArgs)
    {
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(qptArgs);
        when(recordsRequest.getConstraints()).thenReturn(constraints);
    }
}
