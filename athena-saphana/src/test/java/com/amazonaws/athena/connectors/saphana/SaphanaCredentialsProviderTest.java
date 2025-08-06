/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.InvalidParameterException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SaphanaCredentialsProviderTest
{
    // OAuth keys in secret JSON
    private static final String CLIENT_ID_KEY = "client_id";
    private static final String CLIENT_SECRET_KEY = "client_secret";
    private static final String TOKEN_URL_KEY = "token_url";
    private static final String EXPIRES_IN_KEY = "expires_in";
    private static final String FETCHED_AT_KEY = "fetched_at";
    private static final String ACCESS_TOKEN_KEY = "access_token";

    // Basic authentication keys in secret JSON
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";
    private static final String USERNAME_KEY = "username";

    // Test constants for OAuth configuration
    private static final String TEST_SECRET_NAME = "test-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TOKEN_URL = "https://test.hana.ondemand.com/oauth/token";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";

    // Test constants for basic authentication configuration
    private static final String TEST_USERNAME = "test-user";
    private static final String TEST_PASSWORD = "test-password";

    // OAuth response fields
    private static final String TOKEN_TYPE_KEY = "token_type";
    private static final String BEARER_TYPE = "Bearer";

    @Mock
    private SecretsManagerClient mockSecretsClient;

    @Mock
    private HttpClient mockHttpClient;

    private SaphanaCredentialsProvider credentialsProvider;

    @Before
    public void setUp()
    {
        credentialsProvider = new SaphanaCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient, mockHttpClient);
    }

    @Test
    public void testGetCredentialMap_forOAuthConfig()
    {
        try {
            String secretJson = createOAuthSecretJson();
            mockSecretResponse(secretJson);
            mockHttpResponse(200, createTokenResponse());

            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals("", credentialMap.get(USER_KEY));
            assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(PASSWORD_KEY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenStandardCredentials()
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
    public void testGetCredentialMap_whenTokenValid()
    {
        try {
            String secretJson = createOAuthSecretJsonWithValidToken();
            mockSecretResponse(secretJson);

            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals("", credentialMap.get(USER_KEY));
            assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(PASSWORD_KEY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testGetCredentialMap_whenTokenExpired()
    {
        try {
            String secretJson = createOAuthSecretJsonWithExpiredToken();
            mockSecretResponse(secretJson);
            mockHttpResponse(200, createTokenResponse());

            Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
            // Verify that a new token is fetched
            assertNotNull(credentialMap);
            assertEquals("", credentialMap.get(USER_KEY));
            assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(PASSWORD_KEY));
        }
        catch (Exception e) {
            fail("Should not throw exception: " + e.getMessage());
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetCredentialMap_whenHttpRequestFails_throwsException() throws Exception
    {
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);
        mockHttpResponse(400, "{\"error\":\"invalid_client\"}");

        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
            throw e;
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetCredentialMap_whenInvalidSecretJson_throwsException()
    {
        mockSecretResponse("invalid json");
        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
            throw e;
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetCredentialMap_whenSecretNotFound_throwsException()
    {
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().message("Secret not found").build());

        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
            throw e;
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetCredentialMap_whenInvalidParameter_throwsException()
    {
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenThrow(InvalidParameterException.builder().message("Invalid parameter").build());

        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
            throw e;
        }
    }

    @Test
    public void testGetCredentialMap_whenRateLimitExceeded_throwsException() throws IOException, InterruptedException
    {
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);

        // Mock HttpClient to throw IOException
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new IOException("Failed to connect"));

        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
        }
    }

    @Test
    public void testGetCredentialMap_whenInternalError_throwsException() throws IOException, InterruptedException
    {
        String secretJson = createOAuthSecretJson();
        mockSecretResponse(secretJson);

        // Mock HttpClient to throw InterruptedException
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new InterruptedException("Request interrupted"));

        try {
            credentialsProvider.getCredentialMap();
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Error retrieving SAP HANA credentials"));
        }
    }

    private void mockSecretResponse(String secretJson)
    {
        when(mockSecretsClient.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(GetSecretValueResponse.builder().secretString(secretJson).build());
    }

    private String createOAuthSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID_KEY, TEST_CLIENT_ID)
                .put(CLIENT_SECRET_KEY, TEST_CLIENT_SECRET)
                .put(TOKEN_URL_KEY, TEST_TOKEN_URL)
                .toString();
    }

    private String createOAuthSecretJsonWithValidToken()
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID_KEY, TEST_CLIENT_ID)
                .put(CLIENT_SECRET_KEY, TEST_CLIENT_SECRET)
                .put(TOKEN_URL_KEY, TEST_TOKEN_URL)
                .put(USERNAME_KEY, TEST_USERNAME)
                .put(ACCESS_TOKEN_KEY, TEST_ACCESS_TOKEN)
                .put(EXPIRES_IN_KEY, "3600")
                .put(FETCHED_AT_KEY, String.valueOf(System.currentTimeMillis() / 1000 - 1000))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken()
    {
        return new ObjectMapper().createObjectNode()
                .put(CLIENT_ID_KEY, TEST_CLIENT_ID)
                .put(CLIENT_SECRET_KEY, TEST_CLIENT_SECRET)
                .put(TOKEN_URL_KEY, TEST_TOKEN_URL)
                .put(USERNAME_KEY, TEST_USERNAME)
                .put(ACCESS_TOKEN_KEY, "expired-token")
                .put(EXPIRES_IN_KEY, "3600")
                .put(FETCHED_AT_KEY, String.valueOf(System.currentTimeMillis() / 1000 - 4000))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put(USERNAME_KEY, TEST_USERNAME)
                .put(PASSWORD_KEY, TEST_PASSWORD)
                .toString();
    }

    private String createTokenResponse()
    {
        return new ObjectMapper().createObjectNode()
                .put(ACCESS_TOKEN_KEY, TEST_ACCESS_TOKEN)
                .put(TOKEN_TYPE_KEY, BEARER_TYPE)
                .put(EXPIRES_IN_KEY, 3600)
                .toString();
    }

    private void mockHttpResponse(int statusCode, String responseBody) throws IOException, InterruptedException
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.statusCode()).thenReturn(statusCode);
        when(mockResponse.body()).thenReturn(responseBody);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);
    }
}
