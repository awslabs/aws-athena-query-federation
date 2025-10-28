/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019-2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc;

import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;

public class JdbcEnvironmentPropertiesTest
{
    private static final String TEST_HOST = "test.host.com";
    private static final String TEST_PORT = "1234";
    private static final String TEST_DATABASE = "testdb";
    private static final String TEST_SECRET = "testSecret";
    private static final String TEST_ENCRYPT_PARAM = "encrypt=false";
    private static final String CONNECTION_STRING_PREFIX = "databaseName://jdbc:databaseName://";
    private static final String BASE_CONNECTION_STRING = CONNECTION_STRING_PREFIX + TEST_HOST + ":" + TEST_PORT + "/" + TEST_DATABASE;

    private Map<String, String> connectionProperties;
    private JdbcEnvironmentProperties jdbcEnvironmentProperties;

    @Before
    public void setup()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, TEST_HOST);
        connectionProperties.put(PORT, TEST_PORT);
        connectionProperties.put(DATABASE, TEST_DATABASE);
        jdbcEnvironmentProperties = new JdbcEnvironmentProperties() {
            @Override
            protected String getConnectionStringPrefix(Map<String, String> connectionProperties) {
                return CONNECTION_STRING_PREFIX;
            }
        };
    }

    @Test
    public void testConnectionPropertiesWithNoParams() {
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithOnlySecret() {
        connectionProperties.put(SECRET_NAME, TEST_SECRET);
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?${" + TEST_SECRET + "}";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithOnlyJdbcParams() {
        final String sslParam = "ssl=true";
        connectionProperties.put(JDBC_PARAMS, sslParam);
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?" + sslParam;

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithDatabaseAndParamsAndSecret() {
        connectionProperties.put(JDBC_PARAMS, TEST_ENCRYPT_PARAM);
        connectionProperties.put(SECRET_NAME, TEST_SECRET);
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?" + TEST_ENCRYPT_PARAM + "&${" + TEST_SECRET + "}";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testSpecialCharactersInConnectionParams() {
        String specialHost = "test.host-with_special.chars.com";
        String specialDb = "test/db#1";
        String specialParams = "param1=value1&param2=value2#hash;param3=value3";
        
        connectionProperties.put(HOST, specialHost);
        connectionProperties.put(DATABASE, specialDb);
        connectionProperties.put(JDBC_PARAMS, specialParams);
        
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = CONNECTION_STRING_PREFIX + specialHost + ":" + TEST_PORT + "/" + specialDb + "?" + specialParams;
        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testMultipleJdbcParameters() {
        String params = "ssl=true&connectTimeout=10000&socketTimeout=5000&autoReconnect=true&useUnicode=true&characterEncoding=UTF-8";
        connectionProperties.put(JDBC_PARAMS, params);
        
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?" + params;
        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testComplexDatabasePath() {
        String complexDb = "main/schema1/table2";
        connectionProperties.put(DATABASE, complexDb);
        
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = CONNECTION_STRING_PREFIX + TEST_HOST + ":" + TEST_PORT + "/" + complexDb + "?";
        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testSecretWithSpecialCharacters() {
        String specialSecret = "secret/name#1";
        connectionProperties.put(SECRET_NAME, specialSecret);
        
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = BASE_CONNECTION_STRING + "?${" + specialSecret + "}";
        assertEquals(expected, result.get(DEFAULT));
    }
}
