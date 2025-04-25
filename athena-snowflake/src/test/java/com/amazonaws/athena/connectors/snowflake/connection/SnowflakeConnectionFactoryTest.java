/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SnowflakeConnectionFactoryTest
{
    private DatabaseConnectionConfig databaseConnectionConfig;
    private DatabaseConnectionInfo databaseConnectionInfo;
    private Map<String, String> properties;
    private SnowflakeConnectionFactory connectionFactory;
    private CredentialsProvider credentialsProvider;
    private SnowflakePrivateKeyCredentialProvider privateKeyCredentialProvider;

    @Before
    public void setup()
    {
        databaseConnectionConfig = mock(DatabaseConnectionConfig.class);
        when(databaseConnectionConfig.getJdbcConnectionString()).thenReturn("jdbc:snowflake://hostname/?warehouse=warehousename&db=dbname&schema=schemaname");

        databaseConnectionInfo = mock(DatabaseConnectionInfo.class);
        when(databaseConnectionInfo.getDriverClassName()).thenReturn("com.snowflake.client.jdbc.SnowflakeDriver");

        properties = new HashMap<>();
        properties.put("warehouse", "warehousename");
        properties.put("db", "dbname");
        properties.put("schema", "schemaname");

        connectionFactory = new SnowflakeConnectionFactory(databaseConnectionConfig, properties, databaseConnectionInfo);

        credentialsProvider = mock(CredentialsProvider.class);
        DefaultCredentials credentials = new DefaultCredentials("testUser", "testPassword");
        when(credentialsProvider.getCredential()).thenReturn(credentials);

        privateKeyCredentialProvider = mock(SnowflakePrivateKeyCredentialProvider.class);
        DefaultCredentials privateKeyCredentials = new DefaultCredentials("testUser", "testPrivateKey");
        when(privateKeyCredentialProvider.getCredential()).thenReturn(privateKeyCredentials);
    }

    @Test
    public void testGetConnectionWithPasswordAuth() throws SQLException
    {
        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            Connection connection = mock(Connection.class);
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(connection);

            connectionFactory.getConnection(credentialsProvider);

            verify(credentialsProvider, atLeastOnce()).getCredential();
            mockedDriverManager.verify(() -> DriverManager.getConnection(anyString(), any(Properties.class)));
        }
    }

    @Test
    public void testGetConnectionWithPrivateKeyAuth() throws Exception
    {
        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            Connection connection = mock(Connection.class);
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(connection);

            PrivateKey privateKey = mock(PrivateKey.class);
            when(privateKeyCredentialProvider.getPrivateKeyObject()).thenReturn(privateKey);

            connectionFactory.getConnection((CredentialsProvider) privateKeyCredentialProvider);

            verify(privateKeyCredentialProvider, atLeastOnce()).getCredential();
            verify(privateKeyCredentialProvider, atLeastOnce()).getPrivateKeyObject();
            mockedDriverManager.verify(() -> DriverManager.getConnection(anyString(), any(Properties.class)));
        }
    }

    @Test
    public void testGetConnectionWithNullCredentialsProvider()
    {
        SQLException exception = assertThrows(SQLException.class, () -> {
            connectionFactory.getConnection(null);
        });

        assertEquals("CredentialsProvider is null", exception.getMessage());
    }

    @Test
    public void testGetConnectionWithClassNotFoundException() throws SQLException
    {
        DatabaseConnectionInfo invalidInfo = mock(DatabaseConnectionInfo.class);
        when(invalidInfo.getDriverClassName()).thenReturn("invalid.driver.class");

        SnowflakeConnectionFactory testFactory = new SnowflakeConnectionFactory(
                databaseConnectionConfig, properties, invalidInfo);

        // Test execution and verification
        assertThrows(RuntimeException.class, () -> {
            testFactory.getConnection(credentialsProvider);
        });
    }

    @Test
    public void testGetConnectionWithSQLException() throws SQLException
    {
        try (MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {
            SQLException sqlException = new SQLException("Test SQL Exception");
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenThrow(sqlException);

            assertThrows(SQLException.class, () -> {
                connectionFactory.getConnection(credentialsProvider);
            });
        }
    }
}
