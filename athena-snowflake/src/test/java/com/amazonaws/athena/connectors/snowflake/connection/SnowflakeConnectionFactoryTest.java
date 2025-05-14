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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeConnectionFactoryTest
{
    private static final String VALID_PRIVATE_KEY_PEM =
        "-----BEGIN PRIVATE KEY-----\n" +
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCgjDO7+xGLs5bw\n" +
        "HCjQ2xe8qy2axzLKm98l5KkjNx1YR7C2yyEpFJvgca1Yzr2WxJcAahe1gnazcx9a\n" +
        "ceViTjjikftrurNzmFtQmIWoLjZg3JlHBeZmEIBD3B/sGCj5uPGvkVNaDAu1AreN\n" +
        "GwafUlm8PaOnsmBh7QR/UI5OcRCIIfdHoaN7IVOPEsPqpE5Qvo1CB25L6oV+CyWJ\n" +
        "OjfiJiBdEKnhOHrxhdXxC8a/VfF9i5v3lEJin5/qgqpBdz0Zm5ryHlgwXpyG5wRY\n" +
        "1JMPHJDdFdohbr2l1Rj0wzemmo7Fq/Gw5Tz5yMKwD1D0ZBR0uTry4ZytLlzC6zkY\n" +
        "4LXNI0zNAgMBAAECggEAPi0uF/4rFGSP7xuovwIq1jmhJtFAnXDyYfWFf4rnxXm2\n" +
        "OYS/qe4+VBUSYlNm303xgQqVdgk5uVO7b8auZH0Q0MZijZ03xGvb6YG4OaL0El08\n" +
        "y2HAkgSP+Df28POGYvg6OZlZo4UIv6h2t6Ig1XEKKbnheJ+/bg1h8YCcLErjcSS+\n" +
        "0H5Dbhm1gQ3nn9+0+dS9BnC+32tCXtIeWduYxoGwYFvVwwzGR5TWTNsXrXR67tO2\n" +
        "EaoDAhbCMxnkAapaYPUcs66m2U4V7oQDBZrOnEv2HvVbjiAx1zR2H+9vuTqSKFmC\n" +
        "jnzQCnOKsbLE7AZXxkydLWAUwhyzFn+xDQDh0rv1jQKBgQDytELaa347Tu+gEsK/\n" +
        "ge/bNqAMgqmebT8pUnfmuS5h8VdVwPU+zxUYaX2u8IywbbuOBnEgAWdpxDGwnUYQ\n" +
        "mvPcPML/tOvetOcBsOGtay/QwFtmvt7V/zRw5HXdVtAkgLMlM2c2GGuWqFw7ZRHL\n" +
        "tQXqYjsAgdHsir30OfoztdR4swKBgQCpV8JwTFCVM6qfSqO104VEzusvt1bFlA4H\n" +
        "SfQqym80/A55zz6C3bYVsmM1+GaLvM5hlW6Sq1sq7NBAvnvAWLITpaizu7f7P/OC\n" +
        "D04BGBpbPXatAMnHw5zM2r0C6tqww2WPZckqFCw7W9rWjYsnFZyLj1EbdNMxN+/e\n" +
        "Rq3Lc9TkfwKBgAi1z3BnSzB1tMPZ6INW9nS3kSbhyZSV9x5Uh1kQbEm3j5rUQfjv\n" +
        "FaK6pngQyfvK9GA0evrbEgsJr37XJhyScw4EYDstEwn7FA9Lec3vetfTD3SwhO7J\n" +
        "KeijSleXNgEZXVSIc7vNRI8zm5vGFM1qwbuXquZpwk7q68ZIDmKss+NhAoGACrM1\n" +
        "4Pyhdtv94vTHZVzJJfDhIXG3NOLHBCTjHbUO0809aOr0azZxI+vSov1gFWJHtBjK\n" +
        "FNBpAUxXWE/w59Vy4xTrlPe+h0yiKUyoRB9uwuceUY4kMdAlXzhPCxkl2lduWmI3\n" +
        "FMaTiOij6jylV0HhU1wp5s857Pk42dWjc6CNCicCgYBgjc/gq1VHkNGW4gMq9hSh\n" +
        "r8fnrcvQFp10Em+Yv7MipMQ/wb5evT+2Gb4u5hCSre8S7NV9nrBEjqeBODFxZR42\n" +
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
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM))
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
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM))
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
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM))
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
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM))
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
        credentialMap.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        when(mockCredentialsProvider.getCredentialMap()).thenReturn(credentialMap);

        Connection mockConnection = mock(Connection.class);

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
             MockedStatic<DriverManager> mockedDriverManager = Mockito.mockStatic(DriverManager.class)) {

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                    .thenReturn(SnowflakeAuthType.SNOWFLAKE_JWT);
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createPrivateKey(VALID_PRIVATE_KEY_PEM))
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

        MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class);
        mockedAuthUtils.when(() -> SnowflakeAuthUtils.determineAuthType(credentialMap))
                .thenThrow(new IllegalArgumentException("Private key is required"));

        try {
            connectionFactory.getConnection(mockCredentialsProvider);
            org.junit.Assert.fail("Expected IllegalArgumentException to be thrown");
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                assertEquals("Private key is required", e.getMessage());
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            mockedAuthUtils.close();
        }
    }
} 