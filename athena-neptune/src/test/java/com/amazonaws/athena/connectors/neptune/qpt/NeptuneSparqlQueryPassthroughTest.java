/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.qpt;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.COLLECTION;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.DATABASE;
import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.QUERY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneSparqlQueryPassthroughTest {

    private static final String TEST_DB = "testDb";
    private static final String TEST_COLLECTION = "testCollection";
    private static final String VALID_QUERY = "SELECT * WHERE { ?s ?p ?o }";
    private static final String VALID_SCHEMA_FUNCTION = "system.query";

    private final NeptuneSparqlQueryPassthrough queryPassthrough = new NeptuneSparqlQueryPassthrough();
    private Map<String, String> baseArguments;

    @Before
    public void setUp() {
        baseArguments = createArguments();
    }

    private Map<String, String> createArguments() {
        Map<String, String> arguments = new HashMap<>();
        arguments.put(DATABASE, TEST_DB);
        arguments.put(COLLECTION, TEST_COLLECTION);
        arguments.put(QUERY, VALID_QUERY);
        arguments.put(SCHEMA_FUNCTION_NAME, VALID_SCHEMA_FUNCTION);
        return arguments;
    }

    @Test
    public void testVerifyWithValidArguments() {
        try {
            queryPassthrough.verify(baseArguments);
        } catch (Exception e) {
            fail("Should not throw any exception");
        }
    }

    @Test
    public void testVerifyWithEmptyArguments() {
        try {
            queryPassthrough.verify(new HashMap<>());
            fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            assertEquals("Function Signature doesn't match implementation's", e.getMessage());
        }
    }

    @Test
    public void testVerifyWithMissingDatabase() {
        baseArguments.remove(DATABASE);

        try {
            queryPassthrough.verify(baseArguments);
            fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            assertEquals("Missing Query Passthrough Argument: " + DATABASE, e.getMessage());
        }
    }

    @Test
    public void testVerifyWithMissingCollection() {
        baseArguments.remove(COLLECTION);

        try {
            queryPassthrough.verify(baseArguments);
            fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            assertEquals("Missing Query Passthrough Argument: " + COLLECTION, e.getMessage());
        }
    }

    @Test
    public void testVerifyWithMissingQuery() {
        baseArguments.remove(QUERY);

        try {
            queryPassthrough.verify(baseArguments);
            fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            assertEquals("Missing Query Passthrough Argument: " + QUERY, e.getMessage());
        }
    }

    @Test
    public void testVerifyWithTraverseAndQueryArguments_ShouldThrowException() {
        baseArguments.put("TRAVERSE", "g.V().hasLabel('airport')");

        try {
            queryPassthrough.verify(baseArguments);
            fail("Expected AthenaConnectorException");
        } catch (AthenaConnectorException e) {
            assertEquals("Mixed operations not supported: Cannot use both SPARQL query and Gremlin traverse in the same request", e.getMessage());
        }
    }
} 