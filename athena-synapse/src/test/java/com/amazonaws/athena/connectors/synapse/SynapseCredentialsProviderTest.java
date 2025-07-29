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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SynapseCredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-synapse-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TENANT_ID = "test-tenant-id";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";
    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";

    @Mock
    private SecretsManagerClient mockSecretsClient;

    private SynapseCredentialsProvider credentialsProvider;
    private MockedStatic<SecretsManagerClient> mockedSecretsManager;
    private MockedStatic<HttpClient> mockedHttpClientStatic;

    @Before
    public void setUp()
    {
        mockedSecretsManager = mockStatic(SecretsManagerClient.class);
        mockedSecretsManager.when(SecretsManagerClient::create).thenReturn(mockSecretsClient);
        credentialsProvider = new SynapseCredentialsProvider(TEST_SECRET_NAME);
    }

    @After
    public void tearDown()
    {
        if (mockedSecretsManager != null) {
            mockedSecretsManager.close();
        }
        if (mockedHttpClientStatic != null) {
            mockedHttpClientStatic.close();
        }
    }

    @Test
    public void testGetCredentialMapWithOAuthConfig()
    {
        try {
            String secretJson = createOAuthSecretJson();
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(200, createTokenResponse(TEST_ACCESS_TOKEN));
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertNull(credentialMap.get(USER_KEY));
            assertNull(credentialMap.get(PASSWORD_KEY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMapWithUsernamePassword()
    {
        try {
            String secretJson = createStandardSecretJson();
            mockSecretResponse(secretJson);
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertEquals(TEST_USERNAME, credentialMap.get(USER_KEY));
            assertEquals(TEST_PASSWORD, credentialMap.get(PASSWORD_KEY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetOAuthAccessTokenWithValidUnexpiredToken()
    {
        try {
            long now = Instant.now().getEpochSecond();
            String secretJson = createOAuthSecretJsonWithValidToken(now);
            mockSecretResponse(secretJson);
            String token = credentialsProvider.getOAuthAccessToken();
            assertEquals(TEST_ACCESS_TOKEN, token);
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetOAuthAccessTokenWithExpiredTokenFetchesNewToken()
    {
        try {
            long expiredFetchedAt = (Instant.now().getEpochSecond()) - 4000;
            String secretJson = createOAuthSecretJsonWithExpiredToken(expiredFetchedAt);
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(200, createTokenResponse("new-access-token"));
            String token = credentialsProvider.getOAuthAccessToken();
            assertEquals("new-access-token", token);
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetOAuthAccessTokenWithNotOAuthConfigured()
    {
        try {
            String secretJson = createStandardSecretJson();
            mockSecretResponse(secretJson);
            String token = credentialsProvider.getOAuthAccessToken();
            assertNull(token);
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetOAuthAccessTokenWithHttpError() throws Exception
    {
        try {
            long expiredFetchedAt = (Instant.now().getEpochSecond()) - 4000;
            String secretJson = createOAuthSecretJsonWithExpiredToken(expiredFetchedAt);
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(400, "error");
            credentialsProvider.getOAuthAccessToken();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(),
                e.getErrorDetails().errorCode());
        }
    }

    @Test
    public void testGetOAuthAccessTokenWithInvalidJson() throws Exception
    {
        try {
            long expiredFetchedAt = (Instant.now().getEpochSecond()) - 4000;
            String secretJson = createOAuthSecretJsonWithExpiredToken(expiredFetchedAt);
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(200, "not-a-json");
            credentialsProvider.getOAuthAccessToken();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
        }
    }

    @Test
    public void testGetCredentialMapWithInvalidSecret()
    {
        try {
            when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenThrow(new RuntimeException("Secret not found"));
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
        }
    }

    private void mockSecretResponse(String secretJson)
    {
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString(secretJson).build());
    }

    private void mockHttpClientForTokenFetch(int statusCode, String responseBody) throws Exception
    {
        mockedHttpClientStatic = mockStatic(HttpClient.class);
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(statusCode);
        when(mockResponse.body()).thenReturn(responseBody);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(mockResponse);
        mockedHttpClientStatic.when(HttpClient::newHttpClient).thenReturn(mockHttpClient);
    }

    private String createOAuthSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("tenant_id", TEST_TENANT_ID)
                .toString();
    }

    private String createOAuthSecretJsonWithValidToken(long now)
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("tenant_id", TEST_TENANT_ID)
                .put("access_token", TEST_ACCESS_TOKEN)
                .put("expires_in", "3600")
                .put("fetched_at", String.valueOf(now))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken(long expiredFetchedAt)
    {
        return new ObjectMapper().createObjectNode()
                .put("client_id", TEST_CLIENT_ID)
                .put("client_secret", TEST_CLIENT_SECRET)
                .put("tenant_id", TEST_TENANT_ID)
                .put("access_token", "expired-token")
                .put("expires_in", "3600")
                .put("fetched_at", String.valueOf(expiredFetchedAt))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put("username", TEST_USERNAME)
                .put("password", TEST_PASSWORD)
                .toString();
    }

    private String createTokenResponse(String token)
    {
        return String.format("{\"access_token\":\"%s\",\"expires_in\":3600}", token);
    }
}
