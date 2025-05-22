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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;

public class JdbcEnvironmentPropertiesTest
{
    private Map<String, String> connectionProperties;
    private JdbcEnvironmentProperties jdbcEnvironmentProperties;
    @Before
    public void setup()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "test.host.com");
        connectionProperties.put(PORT, "1234");
        connectionProperties.put(DATABASE, "testdb");
        jdbcEnvironmentProperties = new JdbcEnvironmentProperties() {
            @Override
            protected String getConnectionStringPrefix(Map<String, String> connectionProperties) {
                return "databaseName://jdbc:databaseName://";
            }
        };
    }

    @Test
    public void testConnectionPropertiesWithNoParams() {
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = "databaseName://jdbc:databaseName://test.host.com:1234/testdb?";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithOnlySecret() {
        connectionProperties.put(SECRET_NAME, "testSecret");
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = "databaseName://jdbc:databaseName://test.host.com:1234/testdb?${testSecret}";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithOnlyJdbcParams() {
        connectionProperties.put(JDBC_PARAMS, "ssl=true");
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = "databaseName://jdbc:databaseName://test.host.com:1234/testdb?ssl=true";

        assertEquals(expected, result.get(DEFAULT));
    }

    @Test
    public void testConnectionPropertiesWithDatabaseAndParamsAndSecret() {
        connectionProperties.put(JDBC_PARAMS, "encrypt=false");
        connectionProperties.put(SECRET_NAME, "testSecret");
        Map<String, String> result = jdbcEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expected = "databaseName://jdbc:databaseName://test.host.com:1234/testdb?encrypt=false&${testSecret}";

        assertEquals(expected, result.get(DEFAULT));
    }

}
