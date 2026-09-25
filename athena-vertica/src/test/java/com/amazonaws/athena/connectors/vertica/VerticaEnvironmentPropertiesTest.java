/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class VerticaEnvironmentPropertiesTest {
    private Map<String, String> connectionProperties;
    private VerticaEnvironmentProperties verticaEnvironmentProperties;

    @Before
    public void setUp() {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "vertica-cluster-endpoint");
        connectionProperties.put(DATABASE, "verticadb");
        connectionProperties.put(SECRET_NAME, "vertica-secret");
        connectionProperties.put(PORT, "1234");
        verticaEnvironmentProperties = new VerticaEnvironmentProperties();
    }

    @Test
    public void verticaConnectionPropertiesTest() {
        Map<String, String> verticaConnectionProperties = verticaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "vertica://jdbc:vertica://vertica-cluster-endpoint:1234/verticadb?${vertica-secret}";
        assertEquals(expectedConnectionString, verticaConnectionProperties.get(DEFAULT));
    }

    @Test
    public void verticaConnectionPropertiesTest_EmptyStringValues() {
        connectionProperties.put(HOST, "");
        connectionProperties.put(DATABASE, "");
        connectionProperties.put(SECRET_NAME, "");
        connectionProperties.put(PORT, "");
        Map<String, String> result = verticaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "vertica://jdbc:vertica://:/?${}";
        assertEquals(expectedConnectionString, result.get(DEFAULT));
        assertNotNull(result);
    }

    @Test(expected = NullPointerException.class)
    public void verticaConnectionPropertiesTest_NullConnectionProperties_ShouldThrowException() {
        verticaEnvironmentProperties.connectionPropertiesToEnvironment(null);
    }
}
