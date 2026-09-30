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
package com.amazonaws.athena.connectors.neptune;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.CLUSTER_RES_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.GRAPH_TYPE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ClUSTER_RES_ID;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ENDPOINT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_GRAPH_TYPE;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneEnvironmentPropertiesTest {

    private static final String TEST_ENDPOINT = "test-endpoint";
    private static final String TEST_PORT = "8182";
    private static final String TEST_CLUSTER_ID = "test-cluster-id";
    private static final String PROPERTYGRAPH_TYPE = "propertygraph";
    private static final String NULL_VALUE = null;

    private NeptuneEnvironmentProperties properties;
    private Map<String, String> connectionProperties;

    @Before
    public void setUp() {
        properties = new NeptuneEnvironmentProperties();
        connectionProperties = new HashMap<>();
    }

    @Test
    public void connectionPropertiesToEnvironment_withValidProperties_returnsMappedEnvironment() {
        // Create input connection properties
        connectionProperties.put(HOST, TEST_ENDPOINT);
        connectionProperties.put(PORT, TEST_PORT);
        connectionProperties.put(CFG_ClUSTER_RES_ID, TEST_CLUSTER_ID);
        connectionProperties.put(GRAPH_TYPE, PROPERTYGRAPH_TYPE);

        // Create instance and call method
        Map<String, String> result = properties.connectionPropertiesToEnvironment(connectionProperties);

        // Verify results
        assertNotNull("Result should not be null", result);
        assertEquals("Endpoint should match", TEST_ENDPOINT, result.get(CFG_ENDPOINT));
        assertEquals("Port should match", TEST_PORT, result.get(CFG_PORT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withNullValues_handlesNullValuesCorrectly() {
        // Create input connection properties with null values
        connectionProperties.put(HOST, NULL_VALUE);
        connectionProperties.put(PORT, NULL_VALUE);
        connectionProperties.put(CLUSTER_RES_ID, NULL_VALUE);
        connectionProperties.put(GRAPH_TYPE, NULL_VALUE);

        // Create environment map with null values
        Map<String, String> environment = new HashMap<>();
        environment.put(CLUSTER_RES_ID, NULL_VALUE);
        environment.put(GRAPH_TYPE, NULL_VALUE);

        // Create instance and call method
        Map<String, String> result = properties.connectionPropertiesToEnvironment(connectionProperties);

        // Verify results
        assertNotNull("Result should not be null", result);
        assertNull("Endpoint should be null", result.get(CFG_ENDPOINT));
        assertNull("Port should be null", result.get(CFG_PORT));
        assertEquals("Cluster resource ID should be null", environment.get(CLUSTER_RES_ID), result.get(CFG_ClUSTER_RES_ID));
        assertEquals("Graph type should be null", environment.get(GRAPH_TYPE), result.get(CFG_GRAPH_TYPE));
    }
} 