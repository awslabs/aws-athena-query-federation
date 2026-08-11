/*-
 * #%L
 * athena-db2-as400
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
package com.amazonaws.athena.connectors.db2as400;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;

public class Db2As400EnvironmentPropertiesTest
{
    private static final String TEST_SECRET = "testSecret";

    private Map<String, String> connectionProperties;
    private Db2As400EnvironmentProperties db2As400Properties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        db2As400Properties = new Db2As400EnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_withJdbcParamsAndSecret_returnsConnectionString()
    {
        connectionProperties.put(JDBC_PARAMS, "libraries=SAMPLE");
        connectionProperties.put(SECRET_NAME, TEST_SECRET);

        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://localhost;libraries=SAMPLE;:${testSecret}", result.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withHostAndSecret_returnsConnectionString()
    {
        connectionProperties.put(SECRET_NAME, TEST_SECRET);

        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://localhost;:${testSecret}", result.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withJdbcParamsNoSecret_returnsConnectionString()
    {
        connectionProperties.put(JDBC_PARAMS, "prompt=false");

        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://localhost;prompt=false", result.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_withHostOnly_returnsConnectionString()
    {
        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://localhost;", result.get(DEFAULT));
    }

    @Test(expected = NullPointerException.class)
    public void connectionPropertiesToEnvironment_withNullProperties_throwsNullPointerException()
    {
        db2As400Properties.connectionPropertiesToEnvironment(null);
    }

    @Test
    public void connectionPropertiesToEnvironment_withEmptyProperties_returnsConnectionStringWithNullHost()
    {
        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(new HashMap<>());
        assertEquals("db2as400://jdbc:as400://null;", result.get(DEFAULT));
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withEmptyHost_returnsConnectionString()
    {
        connectionProperties.put(HOST, "");
        connectionProperties.put(SECRET_NAME, TEST_SECRET);

        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://;:${testSecret}", result.get(DEFAULT));
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withEmptySecretName_returnsConnectionString()
    {
        connectionProperties.put(SECRET_NAME, "");

        Map<String, String> result = db2As400Properties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("db2as400://jdbc:as400://localhost;:${}", result.get(DEFAULT));
    }
}
