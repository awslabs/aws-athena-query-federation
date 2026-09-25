/*-
 * #%L
 * athena-postgresql
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
package com.amazonaws.athena.connectors.postgresql;

import org.junit.Before;
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

public class PostGreSqlEnvironmentPropertiesTest
{
    private static final String DEFAULT_HOST = "postgres-endpoint";
    private static final String DEFAULT_PORT = "5000";
    private static final String DEFAULT_DATABASE = "testdb";
    private static final String SSL_PARAMS = "ssl=true";

    Map<String, String> connectionProperties;
    PostGreSqlEnvironmentProperties postGreSqlEnvironmentProperties;

    @Before
    public void setUp() {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, DEFAULT_HOST);
        connectionProperties.put(DATABASE, DEFAULT_DATABASE);
        connectionProperties.put(SECRET_NAME, "postgres-secret");
        connectionProperties.put(PORT, DEFAULT_PORT);
        postGreSqlEnvironmentProperties = new PostGreSqlEnvironmentProperties();
    }

    @Test
    public void testBasicConnectionString() {
        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?${postgres-secret}", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testConnectionStringWithJdbcParams() {
        connectionProperties.put(JDBC_PARAMS, SSL_PARAMS);
        connectionProperties.remove(SECRET_NAME);

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?ssl=true", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testConnectionStringWithJdbcParamsAndSecret() {
        connectionProperties.put(JDBC_PARAMS, SSL_PARAMS);

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?ssl=true&${postgres-secret}", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testConnectionStringWithoutOptionalParams() {
        connectionProperties.remove(SECRET_NAME);

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testConnectionStringWithEmptyValues() {
        connectionProperties.put(JDBC_PARAMS, "");
        connectionProperties.put(SECRET_NAME, "");

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?&${}", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testEmptyJdbcParamsWithSecret() {
        connectionProperties.put(JDBC_PARAMS, "");

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?&${postgres-secret}", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testSpecialCharactersInParameters() {
        connectionProperties.put(HOST, "postgres.example.com");
        connectionProperties.put(DATABASE, "test/db");
        connectionProperties.put(JDBC_PARAMS, "currentSchema=public&search_path=public%2Cshared");

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres.example.com:5000/test/db?currentSchema=public&search_path=public%2Cshared&${postgres-secret}", postGreSqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testMultipleJdbcParameters() {
        connectionProperties.put(JDBC_PARAMS, "ssl=true&sslmode=verify-full&connectTimeout=10&socketTimeout=20&tcpKeepAlive=true");

        Map<String, String> postGreSqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("postgres://jdbc:postgresql://postgres-endpoint:5000/testdb?ssl=true&sslmode=verify-full&connectTimeout=10&socketTimeout=20&tcpKeepAlive=true&${postgres-secret}", postGreSqlConnectionProperties.get(DEFAULT));
    }
}