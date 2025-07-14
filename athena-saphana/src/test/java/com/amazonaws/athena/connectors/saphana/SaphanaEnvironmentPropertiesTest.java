/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SaphanaEnvironmentPropertiesTest
{
    private static final String TEST_SECRET_NAME = "saphana-secret";
    private static final String TEST_JDBC_PARAMS = "encrypt=true";
    private static final String EXPECTED_CONNECTION_PREFIX = "saphana://jdbc:sap://";

    private final SaphanaEnvironmentProperties saphanaEnvironmentProperties = new SaphanaEnvironmentProperties();
    private Map<String, String> connectionProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(PORT, "39013");
    }

    @Test
    public void getConnectionStringPrefix_withConnectionProperties_returnsSaphanaJdbcUrlPrefix()
    {
        String prefix = saphanaEnvironmentProperties.getConnectionStringPrefix(connectionProperties);

        assertEquals(EXPECTED_CONNECTION_PREFIX, prefix);
    }

    @Test
    public void getDatabase_withConnectionProperties_returnsDatabaseSeparator()
    {
        String database = saphanaEnvironmentProperties.getDatabase(connectionProperties);

        assertEquals("/", database);
    }

    @Test
    public void connectionPropertiesToEnvironment_withAllProperties_returnsConnectionStringWithSecretAndJdbcParams()
    {
        connectionProperties.put(JDBC_PARAMS, TEST_JDBC_PARAMS);
        connectionProperties.put(SECRET_NAME, TEST_SECRET_NAME);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "saphana://jdbc:sap://localhost:39013/?encrypt=true&${saphana-secret}";
        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withSecretName_returnsConnectionStringWithSecretPlaceholder()
    {
        connectionProperties.put(SECRET_NAME, TEST_SECRET_NAME);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "saphana://jdbc:sap://localhost:39013/?${saphana-secret}";
        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withJdbcParams_returnsConnectionStringWithJdbcParameters()
    {
        connectionProperties.put(JDBC_PARAMS, TEST_JDBC_PARAMS);

        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "saphana://jdbc:sap://localhost:39013/?encrypt=true";
        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withMinimalProperties_returnsConnectionStringWithHostAndPort()
    {
        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "saphana://jdbc:sap://localhost:39013/?";
        assertTrue("Environment should contain the DEFAULT key", environment.containsKey(DEFAULT));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withEmptyHost_returnsConnectionStringWithEmptyHostPlaceholder()
    {
        connectionProperties.put(HOST, "");
        Map<String, String> environment = saphanaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment should contain the DEFAULT key", environment.get(DEFAULT));
        assertTrue("Environment should contain the DEFAULT key", environment.get(DEFAULT).contains(EXPECTED_CONNECTION_PREFIX));
    }

    @Test
    public void getConnectionStringPrefix_withNullConnectionProperties_returnsPrefix()
    {
        String prefix = saphanaEnvironmentProperties.getConnectionStringPrefix(null);
        assertEquals(EXPECTED_CONNECTION_PREFIX, prefix);
    }

    @Test
    public void getDatabase_withNullConnectionProperties_returnsDatabaseSeparator()
    {
        String database = saphanaEnvironmentProperties.getDatabase(null);
        assertEquals("/", database);
    }
} 