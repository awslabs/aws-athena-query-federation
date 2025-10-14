/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.OAuthAccessTokenCredentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.EXPIRES_IN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerOAuthCredentialsProviderTest
{
    protected static final String SECRET_NAME = "test-secret";
    protected static final String TEST_CLIENT_ID = "test-client-id";
    protected static final String TEST_CLIENT_SECRET = "test-client-secret";
    protected static final String TENANT_ID = "tenant_id";
    protected static final String TEST_TENANT_ID = "test-tenant-id";
    protected static final String TEST_ACCESS_TOKEN = "test-access-token";

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private HttpClient httpClient;

    private SqlServerOAuthCredentialsProvider credentialsProvider;
    private ObjectMapper objectMapper;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        CachableSecretsManager cachableSecretsManager = new CachableSecretsManager(secretsManagerClient);
        credentialsProvider = new SqlServerOAuthCredentialsProvider(httpClient);
        credentialsProvider.initialize(SECRET_NAME, new HashMap<>(), cachableSecretsManager);
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testIsOAuthConfigured_WithValidConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        assertTrue(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testIsOAuthConfigured_WithMissingConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        // Missing client_secret and tenant_id

        assertFalse(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testIsOAuthConfigured_WithEmptyValues()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, "");
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        assertFalse(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testBuildTokenRequest()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        var request = credentialsProvider.buildTokenRequest(secretMap);

        assertEquals("POST", request.method());
        assertEquals("application/x-www-form-urlencoded", request.headers().firstValue("Content-Type").get());
        assertTrue(request.uri().toString().contains(TEST_TENANT_ID));
        assertTrue(request.uri().toString().contains("login.microsoftonline.com"));
        assertTrue(request.uri().toString().contains("oauth2/v2.0/token"));
    }

    @Test
    public void testGetCredential_WithValidOAuthConfig() throws IOException, InterruptedException
    {
        // Setup secret with OAuth config
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);
        String secretString = objectMapper.writeValueAsString(secretMap);

        when(secretsManagerClient.getSecretValue(any(GetSecretValueRequest.class)))
            .thenReturn(GetSecretValueResponse.builder().secretString(secretString).build());

        // Setup mock HTTP response with token
        Map<String, String> tokenResponse = new HashMap<>();
        tokenResponse.put(ACCESS_TOKEN, TEST_ACCESS_TOKEN);
        tokenResponse.put(EXPIRES_IN, "3600");
        String responseBody = objectMapper.writeValueAsString(tokenResponse);

        @SuppressWarnings("unchecked")
        HttpResponse<Object> typedResponse = mock(HttpResponse.class);
        when(typedResponse.statusCode()).thenReturn(200);
        when(typedResponse.body()).thenReturn(responseBody);
        when(httpClient.send(any(), any())).thenReturn(typedResponse);

        // Get and verify credential
        var credential = credentialsProvider.getCredential();
        assertTrue(credential instanceof OAuthAccessTokenCredentials);
        assertEquals(TEST_ACCESS_TOKEN, ((OAuthAccessTokenCredentials) credential).getAccessToken());
    }
}
