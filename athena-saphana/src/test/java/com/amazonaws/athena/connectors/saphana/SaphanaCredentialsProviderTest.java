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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SaphanaCredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TOKEN_URL = "https://test.hana.ondemand.com/oauth/token";
    private static final String TEST_USERNAME = "test-user";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";
    private static final String USER_KEY = "user";
    private static final String PASSWORD_KEY = "password";
    private static final String EXPIRES_IN_KEY = "expires_in";
    private static final String FETCHED_AT_KEY = "fetched_at";
    private static final String ACCESS_TOKEN_KEY = "access_token";
    private static final String TOKEN_TYPE_KEY = "token_type";
    private static final String BEARER_TYPE = "Bearer";

    @Mock
    private SecretsManagerClient mockSecretsClient;

    private SaphanaCredentialsProvider credentialsProvider;
    private MockedStatic<SecretsManagerClient> mockedSecretsManager;

    @Before
    public void setUp()
    {
        // Mock SecretsManager client creation
        mockedSecretsManager = mockStatic(SecretsManagerClient.class);
        mockedSecretsManager.when(SecretsManagerClient::create).thenReturn(mockSecretsClient);

        credentialsProvider = new SaphanaCredentialsProvider(TEST_SECRET_NAME);
    }

    @After
    public void tearDown()
    {
        if (mockedSecretsManager != null) {
            mockedSecretsManager.close();
        }
    }

    @Test
    public void testGetCredentialMap_forOAuthConfig()
    {
        try {
            String secretJson = createOAuthSecretJson();
            mockSecretResponse(secretJson);

            try (MockedStatic<SaphanaCredentialsProvider> mockedStatic = Mockito.mockStatic(SaphanaCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SaphanaCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                Map<String, String> credentialMap = credentialsProvider.getCredentialMap();

                assertNotNull(credentialMap);
                assertEquals("", credentialMap.get(USER_KEY));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(PASSWORD_KEY));
            }
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

            try (MockedStatic<SaphanaCredentialsProvider> mockedStatic = Mockito.mockStatic(SaphanaCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SaphanaCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                        .thenReturn(mockConnection);

                Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
                // Verify that a new token is fetched
                assertNotNull(credentialMap);
                assertEquals("", credentialMap.get(USER_KEY));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get(PASSWORD_KEY));
            }
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

        try (MockedStatic<SaphanaCredentialsProvider> mockedStatic = Mockito.mockStatic(SaphanaCredentialsProvider.class)) {
            HttpURLConnection mockConnection = createMockHttpConnection(400, "{\"error\":\"invalid_client\"}");
            mockedStatic.when(() -> SaphanaCredentialsProvider.getHttpURLConnection(anyString(), anyString(), anyString()))
                    .thenReturn(mockConnection);

            try {
                credentialsProvider.getCredentialMap();
                fail("Expected AthenaConnectorException");
            }
            catch (AthenaConnectorException e) {
                assertEquals(e.getErrorDetails().errorCode(), FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString());
                throw e;
            }
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
            assertEquals(e.getErrorDetails().errorCode(), FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString());
            throw e;
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
                .put(SaphanaConstants.CLIENT_ID, TEST_CLIENT_ID)
                .put(SaphanaConstants.CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(SaphanaConstants.TOKEN_URL, TEST_TOKEN_URL)
                .toString();
    }

    private String createOAuthSecretJsonWithValidToken()
    {
        return new ObjectMapper().createObjectNode()
                .put(SaphanaConstants.CLIENT_ID, TEST_CLIENT_ID)
                .put(SaphanaConstants.CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(SaphanaConstants.TOKEN_URL, TEST_TOKEN_URL)
                .put("username", TEST_USERNAME)
                .put(ACCESS_TOKEN_KEY, TEST_ACCESS_TOKEN)
                .put(EXPIRES_IN_KEY, "3600")
                .put(FETCHED_AT_KEY, String.valueOf(System.currentTimeMillis() / 1000 - 1000))
                .toString();
    }

    private String createOAuthSecretJsonWithExpiredToken()
    {
        return new ObjectMapper().createObjectNode()
                .put(SaphanaConstants.CLIENT_ID, TEST_CLIENT_ID)
                .put(SaphanaConstants.CLIENT_SECRET, TEST_CLIENT_SECRET)
                .put(SaphanaConstants.TOKEN_URL, TEST_TOKEN_URL)
                .put("username", TEST_USERNAME)
                .put(ACCESS_TOKEN_KEY, "expired-token")
                .put(EXPIRES_IN_KEY, "3600")
                .put(FETCHED_AT_KEY, String.valueOf(System.currentTimeMillis() / 1000 - 4000))
                .toString();
    }

    private String createStandardSecretJson()
    {
        return new ObjectMapper().createObjectNode()
                .put("username", TEST_USERNAME)
                .put("password", TEST_PASSWORD)
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
