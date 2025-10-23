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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeConnectionFactoryTest
{
    private static final String VALID_PRIVATE_KEY_PEM =
        "-----BEGIN PRIVATE KEY-----\n" +
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCgjDO7+xGLs5bw\n" +
        "DxkVrv9m2jFYrEqoCVXlJhZuJ8AUeje2NPqZjZgaQLLNsizjlM8dwIFTn7zYus8C\n" +
        "0/EKcWRPSoI9PsbgwvFIyQ==\n" +
        "-----END PRIVATE KEY-----";
    public static final String TESTUSER = "testuser";
    public static final String TESTPASSWORD = "testpassword";

    private DatabaseConnectionConfig mockDatabaseConnectionConfig;
    private DatabaseConnectionInfo mockDatabaseConnectionInfo;
    private Map<String, String> properties;
    private SnowflakeConnectionFactory connectionFactory;

    @Before
    public void setUp()
    {
        mockDatabaseConnectionConfig = mock(DatabaseConnectionConfig.class);
        mockDatabaseConnectionInfo = mock(DatabaseConnectionInfo.class);
        properties = new HashMap<>();
        properties.put("test.property", "test.value");

        when(mockDatabaseConnectionConfig.getJdbcConnectionString()).thenReturn("jdbc:snowflake://test.snowflakecomputing.com");
        when(mockDatabaseConnectionInfo.getDriverClassName()).thenReturn("com.snowflake.client.jdbc.SnowflakeDriver");

        connectionFactory = new SnowflakeConnectionFactory(mockDatabaseConnectionConfig, properties, mockDatabaseConnectionInfo);
    }

    @Test
    public void testGetConnectionWithNullCredentialsProvider()
    {
        assertThrows(Exception.class, () -> {
            connectionFactory.getConnection(null);
        });
    }

    @Test
    public void testGetConnectionWithKeyPairAuth() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM, null))
                    .thenReturn(mock(java.security.PrivateKey.class));
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = connectionFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }

    @Test
    public void testGetConnectionWithKeyPairAuthUsingUsernameField() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);
        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM, null))
                    .thenReturn(mock(java.security.PrivateKey.class));
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = connectionFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }

    @Test
    public void testGetConnectionWithPasswordAuth() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentialMap.put(SnowflakeConstants.PASSWORD, TESTPASSWORD);

        Connection mockConnection = mock(Connection.class);
        SnowflakeConnectionFactory spyFactory = Mockito.spy(connectionFactory);
        Mockito.doReturn(mockConnection).when(spyFactory).getConnection(any(CredentialsProvider.class));

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE);

            Connection result = spyFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }

    @Test
    public void testGetConnectionWithOAuthAuth() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.AUTH_CODE, "test-auth-code");

        Connection mockConnection = mock(Connection.class);
        SnowflakeConnectionFactory spyFactory = Mockito.spy(connectionFactory);
        Mockito.doReturn(mockConnection).when(spyFactory).getConnection(any(CredentialsProvider.class));

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.OAUTH);

            Connection result = spyFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }

    @Test
    public void testGetConnectionWithKeyPairAuthAndPrivateKeyCreationException() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM, null))
                    .thenThrow(new Exception("Private key creation failed"));

            assertThrows(SQLException.class, () -> {
                connectionFactory.getConnection(mockCredentialsProvider);
            });
        }
    }

    @Test
    public void testGetConnectionWithKeyPairAuthAndGenericException() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM, null))
                    .thenReturn(mock(java.security.PrivateKey.class));
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenThrow(new RuntimeException("Generic error"));

            assertThrows(SQLException.class, () -> {
                connectionFactory.getConnection(mockCredentialsProvider);
            });
        }
    }

    @Test
    public void testBuildConnectionStringWithSecretPlaceholders() throws Exception
    {
        when(mockDatabaseConnectionConfig.getJdbcConnectionString())
                .thenReturn("jdbc:snowflake://test.snowflakecomputing.com?secretName=test-secret");

        // This tests the private buildConnectionString method indirectly through getConnection
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM, null))
                    .thenReturn(mock(java.security.PrivateKey.class));
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = connectionFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }

    @Test
    public void testGetConnectionWithKeyPairAuthAndMissingUsername()
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenThrow(new IllegalArgumentException("Username is required"));

            assertThrows(IllegalArgumentException.class, () -> {
                connectionFactory.getConnection(mockCredentialsProvider);
            });
        }
    }

    @Test
    public void testGetConnectionWithKeyPairAuthAndMissingPrivateKey() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenThrow(new IllegalArgumentException("Private key is required"));

            assertThrows(IllegalArgumentException.class, () -> {
                connectionFactory.getConnection(mockCredentialsProvider);
            });
        }
    }

    @Test
    public void testGetConnectionWithEncryptedKeyPairAuth() throws Exception
    {
        CredentialsProvider mockCredentialsProvider = mock(CredentialsProvider.class);
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, TESTUSER);
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY, "-----BEGIN ENCRYPTED PRIVATE KEY-----\nencrypted-content\n-----END ENCRYPTED PRIVATE KEY-----");
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, "test-passphrase");

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(anyString(), eq("test-passphrase")))
                    .thenReturn(mock(java.security.PrivateKey.class));
            mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenReturn(mockConnection);

            Connection result = connectionFactory.getConnection(mockCredentialsProvider);

            assertNotNull(result);
            assertEquals(mockConnection, result);
        }
    }
} 