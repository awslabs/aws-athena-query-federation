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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class PropertyGraphHandlerTest
{
    private GraphTraversalSource g;
    private PropertyGraphHandler handler;

    @Before
    public void setUp()
    {
        g = traversal().withEmbedded(TinkerFactory.createModern());
        handler = new PropertyGraphHandler(null);
    }

    @After
    public void tearDown() throws Exception
    {
        if (g != null) {
            g.getGraph().close();
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
}
