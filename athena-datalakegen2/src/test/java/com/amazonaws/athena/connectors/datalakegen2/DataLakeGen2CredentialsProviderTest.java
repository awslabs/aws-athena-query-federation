/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.EXPIRES_IN;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.FETCHED_AT;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.PASSWORD;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.USER;
import static com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider.USERNAME;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants.ACCESS_TOKEN_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataLakeGen2CredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-datalakegen2-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TENANT_ID = "test-tenant-id";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";
    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";
    private static final String TENANT_ID = "tenant_id";

    @Mock
    private SecretsManagerClient mockSecretsClient;

    @Mock
    private HttpClient mockHttpClient;

    private DataLakeGen2CredentialsProvider credentialsProvider;
    private MockedStatic<SecretsManagerClient> mockedSecretsManager;
    private MockedStatic<HttpClient> mockedHttpClient;

    @Before
    public void setUp()
    {
        mockedSecretsManager = mockStatic(SecretsManagerClient.class);
        mockedSecretsManager.when(SecretsManagerClient::create).thenReturn(mockSecretsClient);

        mockedHttpClient = mockStatic(HttpClient.class);
        mockedHttpClient.when(HttpClient::newHttpClient).thenReturn(mockHttpClient);
    }

    @After
    public void tearDown()
    {
        if (mockedSecretsManager != null) {
            mockedSecretsManager.close();
        }
        if (mockedHttpClient != null) {
            mockedHttpClient.close();
        }
    }

    @Test
    public void testGetCredentialMap_whenOAuthConfigured()
    {
        try {
            String secretJson = createOAuthSecretJson();
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(createTokenResponse(TEST_ACCESS_TOKEN));
            credentialsProvider = new DataLakeGen2CredentialsProvider(TEST_SECRET_NAME, mockSecretsClient, mockHttpClient);
            
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertNotNull(credentialMap.get(ACCESS_TOKEN_PROPERTY));
            assertNull(credentialMap.get(USER));
            assertNull(credentialMap.get(PASSWORD));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenValidUnexpiredToken()
    {
        try {
            long now = Instant.now().getEpochSecond();
            String secretJson = createOAuthSecretJsonWithValidToken(now);
            mockSecretResponse(secretJson);
            credentialsProvider = new DataLakeGen2CredentialsProvider(TEST_SECRET_NAME, mockSecretsClient, mockHttpClient);
            
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(ACCESS_TOKEN_PROPERTY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenExpiredToken()
    {
        try {
            long expiredFetchedAt = (Instant.now().getEpochSecond()) - 4000;
            String secretJson = createOAuthSecretJsonWithExpiredToken(expiredFetchedAt);
            mockSecretResponse(secretJson);
            mockHttpClientForTokenFetch(createTokenResponse("new-access-token"));
            credentialsProvider = new DataLakeGen2CredentialsProvider(TEST_SECRET_NAME, mockSecretsClient, mockHttpClient);
            
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertEquals("new-access-token", credentialMap.get(ACCESS_TOKEN_PROPERTY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenUsernamePasswordConfigured()
    {
        try {
            String secretJson = createStandardSecretJson();
            mockSecretResponse(secretJson);
            credentialsProvider = new DataLakeGen2CredentialsProvider(TEST_SECRET_NAME, mockSecretsClient, mockHttpClient);
            
            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            assertNotNull(credentialMap);
            assertNull(credentialMap.get(ACCESS_TOKEN_PROPERTY));
            assertEquals(TEST_USERNAME, credentialMap.get(USER));
            assertEquals(TEST_PASSWORD, credentialMap.get(PASSWORD));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    private void mockSecretResponse(String secretJson)
    {
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString(secretJson).build());
    }

    private void mockHttpClientForTokenFetch(String responseBody)
    {
        try {
            HttpResponse<String> mockResponse = mock(HttpResponse.class);
            when(mockResponse.statusCode()).thenReturn(200);
            when(mockResponse.body()).thenReturn(responseBody);
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to mock HTTP client", e);
        }
    }

    private String createOAuthSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID, TEST_CLIENT_ID)
                .put(CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(TENANT_ID, TEST_TENANT_ID)
                .toString();
    }

    private String createOAuthSecretJsonWithValidToken(long now)
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID, TEST_CLIENT_ID)
                .put(CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(TENANT_ID, TEST_TENANT_ID)
                .put(ACCESS_TOKEN, TEST_ACCESS_TOKEN)
                .put(EXPIRES_IN, "3600")
                .put(FETCHED_AT, String.valueOf(now))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken(long expiredFetchedAt)
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID, TEST_CLIENT_ID)
                .put(CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(TENANT_ID, TEST_TENANT_ID)
                .put(ACCESS_TOKEN, "expired-token")
                .put(EXPIRES_IN, "3600")
                .put(FETCHED_AT, String.valueOf(expiredFetchedAt))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(USERNAME, TEST_USERNAME)
                .put(PASSWORD, TEST_PASSWORD)
                .toString();
    }

    private String createTokenResponse(String token)
    {
        return String.format("{\"%s\":\"%s\",\"%s\":3600}", 
            ACCESS_TOKEN, token, 
            EXPIRES_IN);
    }
}
