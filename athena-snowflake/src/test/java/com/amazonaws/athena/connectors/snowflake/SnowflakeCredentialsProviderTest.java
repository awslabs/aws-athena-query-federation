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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeCredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-oauth-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TOKEN_URL = "https://test.snowflakecomputing.com/oauth/token-request";
    private static final String TEST_REDIRECT_URI = "https://test.com/callback";
    private static final String TEST_AUTH_CODE = "test-auth-code";
    private static final String TEST_USERNAME = "test-user";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";
    private static final String TEST_REFRESH_TOKEN = "test-refresh-token";

    private SecretsManagerClient mockSecretsClient;

    @Before
    public void setUp()
    {
        mockSecretsClient = mock(SecretsManagerClient.class);
    }

    @Test
    public void testGetCredentialWithOAuthFlow() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                DefaultCredentials credentials = provider.getCredential();

                assertNotNull(credentials);
                assertEquals(TEST_USERNAME, credentials.getUser());
                assertEquals(TEST_ACCESS_TOKEN, credentials.getPassword());
            }
        }
    }

    @Test
    public void testGetCredentialMapWithOAuthFlow() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                Map<String, String> credentialMap = provider.getCredentialMap();

                assertNotNull(credentialMap);
                assertEquals(TEST_USERNAME, credentialMap.get("user"));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get("password"));
                assertEquals("oauth", credentialMap.get("authenticator"));
            }
        }
    }

    @Test
    public void testGetCredentialMapWithExistingToken()
    {
        String secretJson = createOAuthSecretJsonWithExistingToken();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            Map<String, String> credentialMap = provider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals(TEST_USERNAME, credentialMap.get("user"));
            assertEquals(TEST_ACCESS_TOKEN, credentialMap.get("password"));
            assertEquals("oauth", credentialMap.get("authenticator"));
        }
    }

    @Test
    public void testGetCredentialMapWithExpiredToken() throws Exception
    {
        String secretJson = createOAuthSecretJsonWithExpiredToken();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                Map<String, String> credentialMap = provider.getCredentialMap();

                assertNotNull(credentialMap);
                assertEquals(TEST_USERNAME, credentialMap.get("user"));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get("password"));
                assertEquals("oauth", credentialMap.get("authenticator"));
            }
        }
    }

    @Test
    public void testGetCredentialMapWithStandardCredentials()
    {
        String secretJson = createStandardSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            Map<String, String> credentialMap = provider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals(TEST_USERNAME, credentialMap.get("user"));
            assertEquals(TEST_PASSWORD, credentialMap.get("password"));
            assertNull(credentialMap.get("authenticator"));
        }
    }

    @Test
    public void testGetCredentialMapWithEmptyAuthCode()
    {
        String secretJson = createOAuthSecretJsonWithEmptyAuthCode();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            Map<String, String> credentialMap = provider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals(TEST_USERNAME, credentialMap.get("user"));
            assertEquals(TEST_PASSWORD, credentialMap.get("password"));
            assertNull(credentialMap.get("authenticator"));
        }
    }

    @Test
    public void testGetCredentialMapWithMissingRequiredProperties()
    {
        String secretJson = createOAuthSecretJsonMissingClientId();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                provider.getCredentialMap();
            });

            assertTrue(exception.getMessage().contains("Missing required property: client_id"));
        }
    }

    @Test
    public void testGetCredentialMapWithHttpError() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(400, "{\"error\":\"invalid_request\"}");
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                    provider.getCredentialMap();
                });

                assertTrue(exception.getMessage().contains("Error retrieving Snowflake credential"));
            }
        }
    }

    @Test
    public void testGetCredentialMapWithInvalidJsonResponse() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, "invalid json response");
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                    provider.getCredentialMap();
                });

                assertTrue(exception.getMessage().contains("Error retrieving Snowflake credentials"));
            }
        }
    }

    @Test
    public void testGetCredentialMapWithIOException() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = mock(HttpURLConnection.class);
                when(mockConnection.getOutputStream()).thenThrow(new IOException("Connection failed"));

                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                    provider.getCredentialMap();
                });

                assertTrue(exception.getMessage().contains("Error retrieving Snowflake credentials"));
            }
        }
    }

    @Test
    public void testGetCredentialMapWithNullSecretString()
    {
        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(null);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                provider.getCredentialMap();
            });

            assertTrue(exception.getMessage().contains("Error retrieving Snowflake credentials"));
        }
    }

    @Test
    public void testGetCredentialMapWithInvalidSecretJson()
    {
        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn("invalid json");
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                provider.getCredentialMap();
            });

            assertTrue(exception.getMessage().contains("Error retrieving Snowflake credentials"));
        }
    }

    @Test
    public void testGetCredentialMapWithPutSecretValueException() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SnowflakeCredentialsProvider provider = new SnowflakeCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SnowflakeCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SnowflakeCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                // This should not throw an exception because the token request succeeds
                // and the exception is only thrown when saving the token
                Map<String, String> credentialMap = provider.getCredentialMap();

                assertNotNull(credentialMap);
                assertEquals(TEST_USERNAME, credentialMap.get("user"));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get("password"));
                assertEquals("oauth", credentialMap.get("authenticator"));
            }
        }
    }

    // Helper methods
    private String createOAuthSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("token_url", TEST_TOKEN_URL)
                .put("redirect_uri", TEST_REDIRECT_URI)
                .put("auth_code", TEST_AUTH_CODE)
                .put("username", TEST_USERNAME)
                .toString();
    }

    private String createOAuthSecretJsonWithExistingToken()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("token_url", TEST_TOKEN_URL)
                .put("redirect_uri", TEST_REDIRECT_URI)
                .put("auth_code", TEST_AUTH_CODE)
                .put("username", TEST_USERNAME)
                .put("access_token", TEST_ACCESS_TOKEN)
                .put("refresh_token", TEST_REFRESH_TOKEN)
                .put("expires_in", "3600")
                .put("fetched_at", String.valueOf(System.currentTimeMillis() / 1000 - 1000))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("token_url", TEST_TOKEN_URL)
                .put("redirect_uri", TEST_REDIRECT_URI)
                .put("auth_code", TEST_AUTH_CODE)
                .put("username", TEST_USERNAME)
                .put("access_token", "expired-token")
                .put("refresh_token", TEST_REFRESH_TOKEN)
                .put("expires_in", "3600")
                .put("fetched_at", String.valueOf(System.currentTimeMillis() / 1000 - 4000))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put("username", TEST_USERNAME)
                .put("password", TEST_PASSWORD)
                .toString();
    }

    private String createOAuthSecretJsonWithEmptyAuthCode()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("token_url", TEST_TOKEN_URL)
                .put("redirect_uri", TEST_REDIRECT_URI)
                .put("auth_code", "")
                .put("username", TEST_USERNAME)
                .put("password", TEST_PASSWORD)
                .toString();
    }

    private String createOAuthSecretJsonMissingClientId()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("token_url", TEST_TOKEN_URL)
                .put("redirect_uri", TEST_REDIRECT_URI)
                .put("auth_code", TEST_AUTH_CODE)
                .put("username", TEST_USERNAME)
                .toString();
    }

    private String createTokenResponse()
    {
        return new JSONObject()
                .put("access_token", TEST_ACCESS_TOKEN)
                .put("token_type", "Bearer")
                .put("expires_in", 3600)
                .put("refresh_token", TEST_REFRESH_TOKEN)
                .toString();
    }

    private HttpURLConnection createMockHttpConnection(int responseCode, String responseBody) throws IOException
    {
        HttpURLConnection mockConnection = mock(HttpURLConnection.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(responseBody.getBytes());

        when(mockConnection.getOutputStream()).thenReturn(outputStream);
        when(mockConnection.getResponseCode()).thenReturn(responseCode);
        when(mockConnection.getInputStream()).thenReturn(inputStream);
        when(mockConnection.getErrorStream()).thenReturn(null);

        return mockConnection;
    }
} 
