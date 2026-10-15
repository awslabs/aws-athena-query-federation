/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeConnectionCredentialsTest
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
    private static final String TEST_EXPIRES_IN_STRING = "3600";
    private static final int TEST_EXPIRES_IN_SECONDS = 3600;
    private static final String REFRESH_TOKEN_JSON_KEY = "refresh_token";
    private static final String TEST_INVALID_SECRET_JSON = "invalid json";
    private static final String TEST_ERROR_RETRIEVING_CREDENTIALS = "Error retrieving Snowflake credentials";
    private static final String TEST_PEM_PRIVATE_KEY =
            "-----BEGIN PRIVATE KEY-----\n"
                    + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCgjDO7+xGLs5bw\n"
                    + "DxkVrv9m2jFYrEqoCVXlJhZuJ8AUeje2NPqZjZgaQLLNsizjlM8dwIFTn7zYus8C\n"
                    + "0/EKcWRPSoI9PsbgwvFIyQ==\n"
                    + "-----END PRIVATE KEY-----";

    private SecretsManagerClient mockSecretsClient;

    @Before
    public void setUp()
    {
        mockSecretsClient = mock(SecretsManagerClient.class);
    }

    @Test
    public void getCredential_OAuthSecretWithAuthCode_ReturnsOAuthJdbcCredentials() throws Exception
    {
        String secretJson = createOAuthSecretJson();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);

        try (MockedStatic<SnowflakeOAuthCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeOAuthCredentialsProvider.class)) {
            HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
            mockedStatic.when(() -> SnowflakeOAuthCredentialsProvider.getHttpURLConnection(any(), any(), any()))
                    .thenReturn(mockConnection);

            CredentialsProvider cp = provider(secretJson, sm);
            assertOAuthJdbcProperties(cp.getCredential().getProperties());
        }
    }

    @Test
    public void getCredentialMap_OAuthSecretWithAuthCode_ReturnsOAuthProperties() throws Exception
    {
        String secretJson = createOAuthSecretJson();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);

        try (MockedStatic<SnowflakeOAuthCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeOAuthCredentialsProvider.class)) {
            HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
            mockedStatic.when(() -> SnowflakeOAuthCredentialsProvider.getHttpURLConnection(any(), any(), any()))
                    .thenReturn(mockConnection);

            assertOAuthJdbcProperties(provider(secretJson, sm).getCredentialMap());
        }
    }

    @Test
    public void getCredentialMap_OAuthSecretWithValidCachedToken_ReturnsCachedTokenWithoutHttpCall()
    {
        String secretJson = createOAuthSecretJsonWithExistingToken();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);
        assertOAuthJdbcProperties(provider(secretJson, sm).getCredentialMap());
    }

    @Test
    public void getCredentialMap_OAuthSecretWithExpiredToken_RefreshesViaHttp() throws Exception
    {
        String secretJson = createOAuthSecretJsonWithExpiredToken();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);

        try (MockedStatic<SnowflakeOAuthCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeOAuthCredentialsProvider.class)) {
            HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
            mockedStatic.when(() -> SnowflakeOAuthCredentialsProvider.getHttpURLConnection(any(), any(), any()))
                    .thenReturn(mockConnection);

            assertOAuthJdbcProperties(provider(secretJson, sm).getCredentialMap());
        }
    }

    @Test
    public void getCredentialMap_PasswordSecret_ReturnsUsernamePassword()
    {
        String secretJson = createStandardSecretJson();
        CachableSecretsManager sm = mock(CachableSecretsManager.class);
        assertPasswordCredentialMap(provider(secretJson, sm).getCredentialMap());
    }

    @Test
    public void getCredentialMap_OAuthSecretWithBlankAuthCode_TreatedAsPasswordAuth()
    {
        String secretJson = createOAuthSecretJsonWithEmptyAuthCode();
        CachableSecretsManager sm = mock(CachableSecretsManager.class);
        assertPasswordCredentialMap(provider(secretJson, sm).getCredentialMap());
    }

    @Test
    public void getCredentialMap_OAuthSecretMissingClientId_FallsBackToDefaultCredentials()
    {
        String secretJson = createOAuthSecretJsonMissingClientId();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);
        assertPasswordCredentialMap(provider(secretJson, sm).getCredentialMap());
    }

    @Test
    public void getCredentialMap_OAuthTokenHttpError_ThrowsRuntimeException() throws Exception
    {
        assertGetCredentialMapThrowsRuntimeContaining(
                createMockHttpConnection(400, "{\"error\":\"invalid_request\"}"),
                TEST_ERROR_RETRIEVING_CREDENTIALS);
    }

    @Test
    public void getCredentialMap_OAuthTokenInvalidJsonResponse_ThrowsRuntimeException() throws Exception
    {
        assertGetCredentialMapThrowsRuntimeContaining(
                createMockHttpConnection(200, "invalid json response"),
                TEST_ERROR_RETRIEVING_CREDENTIALS);
    }

    @Test
    public void getCredentialMap_OAuthTokenIoFailure_ThrowsRuntimeException() throws Exception
    {
        HttpURLConnection mockConnection = mock(HttpURLConnection.class);
        when(mockConnection.getOutputStream()).thenThrow(new IOException("Connection failed"));
        assertGetCredentialMapThrowsRuntimeContaining(mockConnection, TEST_ERROR_RETRIEVING_CREDENTIALS);
    }

    @Test
    public void createProvider_InvalidJson_ThrowsAthenaConnectorException()
    {
        CachableSecretsManager sm = mock(CachableSecretsManager.class);

        assertThrows(AthenaConnectorException.class, () -> {
            provider(TEST_INVALID_SECRET_JSON, sm);
        });
    }

    @Test
    public void createProvider_JwtSecretJson_ReturnsKeyPairCredentialMap()
    {
        String secretJson = new ObjectMapper().createObjectNode()
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .put(SnowflakeConstants.PEM_PRIVATE_KEY, TEST_PEM_PRIVATE_KEY)
                .toString();
        CachableSecretsManager sm = mock(CachableSecretsManager.class);

        Map<String, String> credentialMap = provider(secretJson, sm).getCredentialMap();

        assertEquals(TEST_USERNAME, credentialMap.get(SnowflakeConstants.USER));
        assertEquals(TEST_PEM_PRIVATE_KEY, credentialMap.get(SnowflakeConstants.PEM_PRIVATE_KEY));
    }

    private CachableSecretsManager secretsManagerForOAuth(String json)
    {
        CachableSecretsManager sm = mock(CachableSecretsManager.class);
        when(sm.getSecret(eq(TEST_SECRET_NAME), any())).thenReturn(json);
        when(sm.getSecretsManager()).thenReturn(mockSecretsClient);
        return sm;
    }

    private CredentialsProvider provider(String secretJson, CachableSecretsManager secretsManager)
    {
        return SnowflakeConnectionCredentials.createProvider(TEST_SECRET_NAME, secretJson, secretsManager, null);
    }

    private void assertOAuthJdbcProperties(Map<String, String> props)
    {
        assertNotNull(props);
        assertEquals(TEST_USERNAME, props.get(SnowflakeConstants.USER));
        assertEquals(TEST_ACCESS_TOKEN, props.get(SnowflakeConstants.PASSWORD));
        assertEquals(SnowflakeAuthType.OAUTH.getValue(), props.get(SnowflakeConstants.AUTHENTICATOR));
    }

    private void assertPasswordCredentialMap(Map<String, String> credentialMap)
    {
        assertNotNull(credentialMap);
        assertEquals(TEST_USERNAME, credentialMap.get(SnowflakeConstants.USER));
        assertEquals(TEST_PASSWORD, credentialMap.get(SnowflakeConstants.PASSWORD));
        assertNull(credentialMap.get(SnowflakeConstants.AUTHENTICATOR));
    }

    private void assertGetCredentialMapThrowsRuntimeContaining(HttpURLConnection mockConnection,
            String expectedMessageSubstring)
    {
        String secretJson = createOAuthSecretJson();
        CachableSecretsManager sm = secretsManagerForOAuth(secretJson);
        try (MockedStatic<SnowflakeOAuthCredentialsProvider> mockedStatic = Mockito.mockStatic(SnowflakeOAuthCredentialsProvider.class)) {
            mockedStatic.when(() -> SnowflakeOAuthCredentialsProvider.getHttpURLConnection(any(), any(), any()))
                    .thenReturn(mockConnection);
            RuntimeException exception = assertThrows(RuntimeException.class, () -> provider(secretJson, sm).getCredentialMap());
            assertTrue(exception.getMessage().contains(expectedMessageSubstring));
        }
    }

    private ObjectNode baseOAuthObjectNode()
    {
        return new ObjectMapper().createObjectNode()
                .put(SnowflakeConstants.CLIENT_ID, TEST_CLIENT_ID)
                .put(SnowflakeConstants.CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(SnowflakeConstants.TOKEN_URL, TEST_TOKEN_URL)
                .put(SnowflakeConstants.REDIRECT_URI, TEST_REDIRECT_URI);
    }

    private String createOAuthSecretJson()
    {
        return baseOAuthObjectNode()
                .put(SnowflakeConstants.AUTH_CODE, TEST_AUTH_CODE)
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .toString();
    }

    private String createOAuthSecretJsonWithExistingToken()
    {
        return baseOAuthObjectNode()
                .put(SnowflakeConstants.AUTH_CODE, TEST_AUTH_CODE)
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .put(CredentialsConstants.ACCESS_TOKEN, TEST_ACCESS_TOKEN)
                .put(REFRESH_TOKEN_JSON_KEY, TEST_REFRESH_TOKEN)
                .put(CredentialsConstants.EXPIRES_IN, TEST_EXPIRES_IN_STRING)
                .put(CredentialsConstants.FETCHED_AT, String.valueOf(System.currentTimeMillis() / 1000 - 1000))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken()
    {
        return baseOAuthObjectNode()
                .put(SnowflakeConstants.AUTH_CODE, TEST_AUTH_CODE)
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .put(CredentialsConstants.ACCESS_TOKEN, "expired-token")
                .put(REFRESH_TOKEN_JSON_KEY, TEST_REFRESH_TOKEN)
                .put(CredentialsConstants.EXPIRES_IN, TEST_EXPIRES_IN_STRING)
                .put(CredentialsConstants.FETCHED_AT, String.valueOf(System.currentTimeMillis() / 1000 - 4000))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .put(SnowflakeConstants.PASSWORD, TEST_PASSWORD)
                .toString();
    }

    private String createOAuthSecretJsonWithEmptyAuthCode()
    {
        return baseOAuthObjectNode()
                .put(SnowflakeConstants.AUTH_CODE, "")
                .put(SnowflakeConstants.USERNAME, TEST_USERNAME)
                .put(SnowflakeConstants.PASSWORD, TEST_PASSWORD)
                .toString();
    }

    private String createOAuthSecretJsonMissingClientId()
    {
        ObjectNode node = baseOAuthObjectNode();
        node.remove(SnowflakeConstants.CLIENT_ID);
        node.put(SnowflakeConstants.AUTH_CODE, TEST_AUTH_CODE);
        node.put(SnowflakeConstants.USERNAME, TEST_USERNAME);
        node.put(SnowflakeConstants.PASSWORD, TEST_PASSWORD);
        return node.toString();
    }

    private String createTokenResponse()
    {
        return new ObjectMapper().createObjectNode()
                .put(CredentialsConstants.ACCESS_TOKEN, TEST_ACCESS_TOKEN)
                .put("token_type", "Bearer")
                .put(CredentialsConstants.EXPIRES_IN, TEST_EXPIRES_IN_SECONDS)
                .put(REFRESH_TOKEN_JSON_KEY, TEST_REFRESH_TOKEN)
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
