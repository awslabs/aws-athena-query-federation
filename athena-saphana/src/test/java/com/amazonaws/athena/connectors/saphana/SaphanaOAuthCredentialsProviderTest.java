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

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.OAuthAccessTokenCredentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.EXPIRES_IN;
import static com.amazonaws.athena.connectors.saphana.SaphanaOAuthCredentialsProvider.TOKEN_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SaphanaOAuthCredentialsProviderTest
{
    protected static final String SECRET_NAME = "test-secret";
    protected static final String TEST_CLIENT_ID = "test-client-id";
    protected static final String TEST_CLIENT_SECRET = "test-client-secret";
    protected static final String TEST_TOKEN_URL = "https://12345.authentication.us10.hana.ondemand.com/oauth/token";
    protected static final String TEST_ACCESS_TOKEN = "test-access-token";

    @Mock
    private SecretsManagerClient secretsManagerClient;

    @Mock
    private HttpClient httpClient;

    private SaphanaOAuthCredentialsProvider credentialsProvider;
    private ObjectMapper objectMapper;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        CachableSecretsManager cachableSecretsManager = new CachableSecretsManager(secretsManagerClient);
        
        // Setup proper OAuth config for tests that need it
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TOKEN_URL, TEST_TOKEN_URL);
        
        credentialsProvider = new SaphanaOAuthCredentialsProvider(httpClient);
        credentialsProvider.initialize(SECRET_NAME, secretMap, cachableSecretsManager);
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testIsOAuthConfigured_WithValidConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TOKEN_URL, TEST_TOKEN_URL);

        assertTrue(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testIsOAuthConfigured_WithMissingConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        // Missing client_secret and token_url

        assertFalse(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testBuildTokenRequest()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TOKEN_URL, TEST_TOKEN_URL);

        var request = credentialsProvider.buildTokenRequest(secretMap);

        assertEquals("POST", request.method());
        assertEquals("application/x-www-form-urlencoded", request.headers().firstValue("Content-Type").get());
        assertTrue(request.uri().toString().contains(TEST_TOKEN_URL));
    }

    @Test
    public void testGetCredential_WithValidOAuthConfig() throws IOException, InterruptedException
    {
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
        Credentials credential = credentialsProvider.getCredential();
        assertTrue("Should return SAP HANA specific credentials", credential instanceof SaphanaOAuthAccessTokenCredentials);
        
        SaphanaOAuthAccessTokenCredentials saphanaCredentials = (SaphanaOAuthAccessTokenCredentials) credential;
        assertEquals("Access token should match", TEST_ACCESS_TOKEN, saphanaCredentials.getAccessToken());
        
        // Verify SAP HANA specific property mapping
        Map<String, String> properties = credential.getProperties();
        assertEquals("User should be empty for OAuth", "", properties.get(CredentialsConstants.USER));
        assertEquals("Password should contain access token", TEST_ACCESS_TOKEN, properties.get(CredentialsConstants.PASSWORD));
    }

    @Test
    public void testSaphanaOAuthCredentials_UsesPasswordProperty()
    {
        // Create SAP HANA OAuth credentials directly
        SaphanaOAuthAccessTokenCredentials saphanaCredentials = new SaphanaOAuthAccessTokenCredentials(TEST_ACCESS_TOKEN);
        Map<String, String> saphanaProperties = saphanaCredentials.getProperties();
        
        // Create standard OAuth credentials for comparison
        OAuthAccessTokenCredentials standardCredentials = new OAuthAccessTokenCredentials(TEST_ACCESS_TOKEN);
        Map<String, String> standardProperties = standardCredentials.getProperties();
        
        // Verify the difference in property mapping
        // Standard OAuth uses "accessToken" property
        assertEquals("Standard OAuth should use accessToken property", TEST_ACCESS_TOKEN, 
            standardProperties.get(CredentialsConstants.ACCESS_TOKEN_PROPERTY));
        
        // SAP HANA OAuth uses "password" property  
        assertEquals("SAP HANA OAuth should use password property", TEST_ACCESS_TOKEN, 
            saphanaProperties.get(CredentialsConstants.PASSWORD));
        assertEquals("SAP HANA OAuth should have empty user", "", 
            saphanaProperties.get(CredentialsConstants.USER));
            
        // Verify SAP HANA does NOT use the standard accessToken property
        assertFalse("SAP HANA should not contain accessToken property", saphanaProperties.containsKey(CredentialsConstants.ACCESS_TOKEN_PROPERTY));
    }
}
