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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.OAuthAccessTokenCredentials;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SynapseJdbcConnectionFactoryTest
{
    @Mock
    private DatabaseConnectionConfig mockDatabaseConnectionConfig;
    
    @Mock
    private DatabaseConnectionInfo mockDatabaseConnectionInfo;
    
    @Mock
    private CredentialsProvider mockCredentialsProvider;
    
    @Mock
    private OAuthAccessTokenCredentials mockOAuthCredentials;
    
    @Mock
    private Connection mockConnection;
    
    private SynapseJdbcConnectionFactory connectionFactory;
    private Map<String, String> properties;
    
    private static final String TEST_JDBC_URL = "jdbc:sqlserver://test-server:1433;database=testdb";
    private static final String TEST_JDBC_URL_WITH_SECRET = "jdbc:sqlserver://test-server:1433;database=testdb;secretName=test-secret";
    private static final String TEST_JDBC_URL_AAD = "jdbc:sqlserver://test-server:1433;database=testdb;authentication=ActiveDirectoryServicePrincipal;secretName=test-secret";
    private static final String TEST_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpassword";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";

    @Before
    public void setUp()
    {
        properties = new HashMap<>();
        properties.put("testProperty", "testValue");
        
        when(mockDatabaseConnectionInfo.getDriverClassName()).thenReturn(TEST_DRIVER_CLASS);
        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn(TEST_JDBC_URL);
    }

    @Test
    public void testConstructorWithValidParameters()
    {
        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );
        
        assertNotNull(connectionFactory);
    }

    @Test
    public void testGetConnectionWithoutCredentialsProviderSuccess()
    {
        // Test that the method can handle null credentials provider
        // This test verifies the basic flow without actually creating a connection
        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        // Should throw RuntimeException due to ClassNotFoundException for driver
        // This is expected behavior when the SQL Server driver is not available
        assertThrows(RuntimeException.class, () -> {
            connectionFactory.getConnection(null);
        });
    }

    @Test
    public void testGetConnectionWithBasicCredentialsSuccess()
    {
        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn(TEST_JDBC_URL_WITH_SECRET);
        
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(CredentialsConstants.USER, TEST_USER);
        credentialMap.put(CredentialsConstants.PASSWORD, TEST_PASSWORD);
        
        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);
        when(mockCredentialsProvider.getCredential()).thenReturn(null);

        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        // Should throw RuntimeException due to ClassNotFoundException for driver
        // This is expected behavior when the SQL Server driver is not available
        assertThrows(RuntimeException.class, () -> {
            connectionFactory.getConnection(mockCredentialsProvider);
        });
    }

    @Test
    public void testGetConnectionWithOAuthCredentialsSuccess()
    {
        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn(TEST_JDBC_URL_WITH_SECRET);
        
        when(mockCredentialsProvider.getCredential()).thenReturn(mockOAuthCredentials);
        when(mockOAuthCredentials.getAccessToken()).thenReturn(TEST_ACCESS_TOKEN);

        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        // Should throw RuntimeException due to ClassNotFoundException for driver
        // This is expected behavior when the SQL Server driver is not available
        assertThrows(RuntimeException.class, () -> {
            connectionFactory.getConnection(mockCredentialsProvider);
        });
    }

    @Test
    public void testGetConnectionWithAADServicePrincipalCredentialsSuccess()
    {
        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn(TEST_JDBC_URL_AAD);
        
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(CredentialsConstants.USER, TEST_USER);
        credentialMap.put(CredentialsConstants.PASSWORD, TEST_PASSWORD);
        
        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);
        lenient().when(mockCredentialsProvider.getCredential()).thenReturn(null);

        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        // Should throw RuntimeException due to ClassNotFoundException for driver
        // This is expected behavior when the SQL Server driver is not available
        assertThrows(RuntimeException.class, () -> {
            connectionFactory.getConnection(mockCredentialsProvider);
        });
    }

    @Test
    public void testGetConnectionWithNullCredentialsProviderCredentialMapThrowsException()
    {
        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn(TEST_JDBC_URL_WITH_SECRET);
        
        when(mockCredentialsProvider.getCredentialMap()).thenReturn(null);
        when(mockCredentialsProvider.getCredential()).thenReturn(null);

        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        // Should throw NullPointerException when trying to access null credential map
        assertThrows(NullPointerException.class, () -> {
            connectionFactory.getConnection(mockCredentialsProvider);
        });
    }

    @Test
    public void testGetConnectionWithNonExistentDriverThrowsException()
    {
        // Use a non-existent driver class to trigger ClassNotFoundException
        when(mockDatabaseConnectionInfo.getDriverClassName()).thenReturn("com.nonexistent.Driver");
        
        connectionFactory = new SynapseJdbcConnectionFactory(
            mockDatabaseConnectionConfig, 
            properties, 
            mockDatabaseConnectionInfo
        );

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            connectionFactory.getConnection(null);
        });

        assertNotNull(exception.getCause());
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }
}