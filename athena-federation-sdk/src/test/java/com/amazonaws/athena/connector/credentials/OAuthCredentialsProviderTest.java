/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN_PROPERTY;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.EXPIRES_IN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.FETCHED_AT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OAuthCredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private SecretsManagerClient mockSecretsClient;

    @Mock
    private HttpClient mockHttpClient;

    private TestOAuthCredentialsProvider credentialsProvider;
    private Map<String, String> secretMap;

    @Before
    public void setUp()
    {
        secretMap = new HashMap<>();
        credentialsProvider = new TestOAuthCredentialsProvider(mockHttpClient);
        credentialsProvider.initialize(TEST_SECRET_NAME, secretMap, new CachableSecretsManager(mockSecretsClient));
    }

    @Test
    public void testGetCredentialMap_whenValidUnexpiredToken() throws Exception
    {
        // Setup valid token in secret map
        long now = Instant.now().getEpochSecond();
        String secretJson = createOAuthSecretJsonWithValidToken(now);
        mockSecretResponse(secretJson);
        
        Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
        assertNotNull(credentialMap);
        assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(ACCESS_TOKEN_PROPERTY));
    }

    @Test
    public void testGetCredentialMap_whenExpiredToken() throws Exception
    {
        // Setup expired token in secret map
        long expiredFetchedAt = (Instant.now().getEpochSecond()) - 4000;
        String secretJson = createOAuthSecretJsonWithExpiredToken(expiredFetchedAt);
        mockSecretResponse(secretJson);
        mockHttpClientForTokenFetch(createTokenResponse());
        
        Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
        assertNotNull(credentialMap);
        assertEquals("new-access-token", credentialMap.get(ACCESS_TOKEN_PROPERTY));
    }

    @Test
    public void testGetCredentialMap_whenRequiredFieldsMissing_throwsException() throws IOException, InterruptedException
    {
        // Setup initial OAuth config
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);
        mockHttpClientForTokenFetch("{\"some_field\":\"value\"}");
        
        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
            assertEquals("Response missing access_token or expires_in fields", 
                e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenIOException_throwsException() throws IOException, InterruptedException
    {
        // Setup initial OAuth config
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);
        
        String errorMessage = "Network error";
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new IOException(errorMessage));
        
        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
            assertEquals(errorMessage, e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenInterruptedException_throwsException() throws IOException, InterruptedException
    {
        // Setup initial OAuth config
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);
        
        String errorMessage = "Operation interrupted";
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new InterruptedException(errorMessage));
        
        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
            assertEquals(errorMessage, e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenSecretNotFound_throwsException() throws IOException, InterruptedException
    {
        // Force a SecretsManager ResourceNotFoundException during secret update
        secretMap.clear();
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"access_token\":\"abc\",\"expires_in\":3600}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        when(mockSecretsClient.putSecretValue(any(Consumer.class)))
                .thenThrow(software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException.builder()
                        .message("secret missing").build());

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("secret missing", e.getErrorDetails().errorMessage());
        }
    }
    @Test
    public void testGetCredential_whenJsonProcessingException_throwsException() throws IOException, InterruptedException
    {
        // Break JSON parsing deliberately by mocking HttpResponse with invalid JSON
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("not-json");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            // message from Jackson parse exception
            assertNotNull(e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenHttpUnauthorized_throwsInvalidCredentials() throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(401);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("HTTP Status: 401", e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenHttpRateLimited_throwsThrottlingException() throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(429);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("HTTP Status: 429", e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenHttpUnexpectedStatus_throwsInvalidResponse() throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(500);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("Unexpected HTTP status: 500", e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenSecretUpdateFailsWithResourceNotFound() throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"access_token\":\"abc\",\"expires_in\":3600}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        when(mockSecretsClient.putSecretValue(any(Consumer.class)))
                .thenThrow(software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException.builder()
                        .message("secret not found").build());

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("secret not found", e.getErrorDetails().errorMessage());
        }
    }

    @Test
    public void testGetCredential_whenSecretUpdateFailsWithSecretsManagerException() throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"access_token\":\"abc\",\"expires_in\":3600}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        when(mockSecretsClient.putSecretValue(any(Consumer.class)))
                .thenThrow(SecretsManagerException.builder().message("SM failure").build());

        try {
            credentialsProvider.getCredential();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertEquals("SM failure", e.getErrorDetails().errorMessage());
        }
    }

    private void mockSecretResponse(String secretJson) throws IOException
    {
        // Only update the secret map for OAuth cases
        if (secretJson.contains(CLIENT_ID) || secretJson.contains(ACCESS_TOKEN)) {
            secretMap.clear();
            secretMap.putAll(OBJECT_MAPPER.readValue(secretJson, Map.class));
        }
    }

    private void mockHttpClientForTokenFetch(String responseBody) throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn(responseBody);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);
    }

    private String createOAuthSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID, TEST_CLIENT_ID)
                .put(CLIENT_SECRET, TEST_CLIENT_SECRET)
                .toString();
    }

    private String createOAuthSecretJsonWithValidToken(long now)
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID, TEST_CLIENT_ID)
                .put(CLIENT_SECRET, TEST_CLIENT_SECRET)
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
                .put(ACCESS_TOKEN, "expired-token")
                .put(EXPIRES_IN, "3600")
                .put(FETCHED_AT, String.valueOf(expiredFetchedAt))
                .toString();
    }

    private String createTokenResponse()
    {
        return String.format("{\"%s\":\"%s\",\"%s\":3600}", 
            ACCESS_TOKEN, "new-access-token",
            EXPIRES_IN);
    }

    private static class TestOAuthCredentialsProvider extends OAuthCredentialsProvider
    {
        public TestOAuthCredentialsProvider()
        {
            super();
        }
        
        public TestOAuthCredentialsProvider(HttpClient httpClient)
        {
            super(httpClient);
        }

        @Override
        protected boolean isOAuthConfigured(Map<String, String> secretMap)
        {
            return secretMap.containsKey(CLIENT_ID) &&
                   !secretMap.get(CLIENT_ID).isEmpty() &&
                   secretMap.containsKey(CLIENT_SECRET) &&
                   !secretMap.get(CLIENT_SECRET).isEmpty();
        }

        @Override
        protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
        {
            return HttpRequest.newBuilder().uri(java.net.URI.create("http://test")).build();
        }
    }
}
