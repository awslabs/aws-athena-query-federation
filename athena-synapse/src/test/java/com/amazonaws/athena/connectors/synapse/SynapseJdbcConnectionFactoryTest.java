/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SynapseJdbcConnectionFactoryTest
{
    private static final String CATALOG = "testCatalog";
    private static final String SYNAPSE = "synapse";
    private static final String SECRET_NAME = "testSecret";
    private static final String MOCK_JDBC_URL = "jdbc:sqlserver://sql.azuresynapse.net:1433;databaseName=testdb";
    private static final int MOCK_PORT = 1433;
    private static final String MOCK_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String TEST_USERNAME = "testUser";
    private static final String TEST_PASSWORD = "testPassword";
    private static final String SECRET_PLACEHOLDER = ";${test}";
    private static final String USER_PASSWORD_PARAMS = ";user=" + TEST_USERNAME + ";password=" + TEST_PASSWORD;
    private static final String ACTIVE_DIRECTORY_AUTH = ";authentication=ActiveDirectoryServicePrincipal";
    private static final String AAD_PRINCIPAL_PARAMS = ";AADSecurePrincipalId=" + TEST_USERNAME + ";AADSecurePrincipalSecret=" + TEST_PASSWORD;

    private Driver mockDriver;
    private Connection mockConnection;
    private AtomicReference<String> actualUrl;

    @Before
    public void setUp() throws SQLException
    {
        mockDriver = mock(Driver.class);
        mockConnection = mock(Connection.class);
        actualUrl = new AtomicReference<>();

        when(mockDriver.acceptsURL(any())).thenReturn(true);
        when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
            actualUrl.set(invocation.getArgument(0));
            return mockConnection;
        });

        DriverManager.registerDriver(mockDriver);
    }

    @After
    public void tearDown() throws SQLException
    {
        if (mockDriver != null) {
            DriverManager.deregisterDriver(mockDriver);
        }
    }

    private Connection createConnection(String jdbcUrl, CredentialsProvider credentialsProvider)
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(CATALOG, SYNAPSE, jdbcUrl, SECRET_NAME);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(MOCK_DRIVER_CLASS, MOCK_PORT);
        SynapseJdbcConnectionFactory factory = new SynapseJdbcConnectionFactory(config, new HashMap<>(), info);
        return factory.getConnection(credentialsProvider);
    }

    @Test
    public void testGetConnection_withCredentials_replacesSecret()
    {
        Connection connection = createConnection(MOCK_JDBC_URL + SECRET_PLACEHOLDER,
                new StaticCredentialsProvider(new DefaultCredentials(TEST_USERNAME, TEST_PASSWORD)));
        String expectedUrl = MOCK_JDBC_URL + USER_PASSWORD_PARAMS;
        assertEquals(expectedUrl, actualUrl.get());
        assertEquals(mockConnection, connection);
    }

    @Test
    public void testGetConnection_withActiveDirectoryServicePrincipal()
    {
        Connection connection = createConnection(
                MOCK_JDBC_URL + ACTIVE_DIRECTORY_AUTH + SECRET_PLACEHOLDER,
                new StaticCredentialsProvider(new DefaultCredentials(TEST_USERNAME, TEST_PASSWORD)));
        String expectedUrl = MOCK_JDBC_URL + ACTIVE_DIRECTORY_AUTH + AAD_PRINCIPAL_PARAMS;
        assertEquals(expectedUrl, actualUrl.get());
        assertEquals(mockConnection, connection);
    }

    @Test
    public void testGetConnection_withNullCredentials_doesNotReplaceSecret()
    {
        Connection connection = createConnection(MOCK_JDBC_URL + USER_PASSWORD_PARAMS, null);
        assertEquals(MOCK_JDBC_URL + USER_PASSWORD_PARAMS, actualUrl.get());
        assertEquals(mockConnection, connection);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_whenDriverClassNotFound_throwsRuntimeException() throws SQLException
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(CATALOG, SYNAPSE, MOCK_JDBC_URL, SECRET_NAME);

        // Intentionally provide invalid driver class
        DatabaseConnectionInfo info = new DatabaseConnectionInfo("invalid.DriverClass", MOCK_PORT);

        SynapseJdbcConnectionFactory factory = new SynapseJdbcConnectionFactory(config, new HashMap<>(), info);
        try (Connection ignored = factory.getConnection(null)) {
            fail("Expected RuntimeException was not thrown");
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_whenDriverFailsToConnect_throwsRuntimeException() throws SQLException
    {
        // Override the default mock behavior for this specific test
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenThrow(new SQLException("DB error", "08001", 999));

        try (Connection ignored = createConnection(MOCK_JDBC_URL, 
                new StaticCredentialsProvider(new DefaultCredentials(TEST_USERNAME, TEST_PASSWORD)))) {
            fail("Expected RuntimeException was not thrown");
        }
    }
}
