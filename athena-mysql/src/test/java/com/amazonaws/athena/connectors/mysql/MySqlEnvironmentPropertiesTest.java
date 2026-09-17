/*-
 * #%L
 * athena-mysql
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
package com.amazonaws.athena.connectors.mysql;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MySqlEnvironmentPropertiesTest {

    private final MySqlEnvironmentProperties environmentProperties = new MySqlEnvironmentProperties();
    private static final String expectedPrefix = "mysql://jdbc:mysql://";

    @Test
    public void getConnectionStringPrefix_withValidConnectionProperties_returnsMySqlJdbcUrlPrefix() {
        Map<String, String> connectionProperties = getConnectionProperties();
        String actualPrefix = environmentProperties.getConnectionStringPrefix(connectionProperties);
        assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void getConnectionStringPrefix_withEmptyConnectionProperties_returnsMySqlJdbcUrlPrefix() {
        Map<String, String> connectionProperties = new HashMap<>();
        String actualPrefix = environmentProperties.getConnectionStringPrefix(connectionProperties);
        assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void getConnectionStringPrefix_withNullConnectionProperties_returnsMySqlJdbcUrlPrefix() {
        String actualPrefix = environmentProperties.getConnectionStringPrefix(null);
        assertEquals(expectedPrefix, actualPrefix);
    }

    @Test
    public void connectionPropertiesToEnvironment_withSecretName_returnsConnectionStringWithSecretPlaceholder() {
        Map<String, String> connectionProperties = getConnectionProperties();
        connectionProperties.put(SECRET_NAME, "test_secret_name");
        Map<String, String> environment = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "mysql://jdbc:mysql://localhost:3306/testdb?${test_secret_name}";

        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withUserAndPassword_returnsConnectionStringWithJdbcParams() {
        Map<String, String> connectionProperties = getConnectionProperties();
        connectionProperties.put(JDBC_PARAMS, "user=sample&password=sample1");
        Map<String, String> environment = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "mysql://jdbc:mysql://localhost:3306/testdb?user=sample&password=sample1";

        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test(expected = NullPointerException.class)
    public void connectionPropertiesToEnvironment_withNullConnectionProperties_throwsNullPointerException() {
        environmentProperties.connectionPropertiesToEnvironment(null);
    }

    private Map<String, String> getConnectionProperties() {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(PORT, "3306");
        connectionProperties.put(DATABASE, "testdb");
        return connectionProperties;
    }
} 