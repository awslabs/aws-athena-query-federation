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

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.EXPIRES_IN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.FETCHED_AT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeOAuthCredentialsProviderTest
{
    private static final String OAUTH_TEST_USER = "snow-user";
    private static final String OAUTH_TEST_TOKEN_URL = "https://acct.snowflakecomputing.com/oauth/token-request";
    private static final String OAUTH_TEST_REDIRECT = "https://cb";

    private SnowflakeOAuthCredentialsProvider provider;

    @Before
    public void setUp()
    {
        provider = new SnowflakeOAuthCredentialsProvider();
    }

    @Test
    public void isOAuthConfigured_BlankAuthCode_ReturnsFalse()
    {
        assertFalse(provider.isOAuthConfigured(mapWithAuthCodeValue("   ")));
    }

    @Test
    public void isOAuthConfigured_OnlyAuthCode_ReturnsFalse()
    {
        assertFalse(provider.isOAuthConfigured(mapWithAuthCodeValue("code")));
    }

    @Test
    public void isOAuthConfigured_AllRequiredOAuthFields_ReturnsTrue()
    {
        assertTrue(provider.isOAuthConfigured(secretMapWithAuthCode("code")));
    }

    @Test
    public void isOAuthConfigured_MissingRedirectUri_ReturnsFalse()
    {
        Map<String, String> map = secretMapWithAuthCode("c");
        map.remove(SnowflakeConstants.REDIRECT_URI);
        assertFalse(provider.isOAuthConfigured(map));
    }

    @Test
    public void buildTokenRequest_AnySecretMap_ThrowsUnsupportedOperationException()
    {
        assertThrows(UnsupportedOperationException.class, () -> provider.buildTokenRequest(new HashMap<>()));
    }

    @Test
    public void getCredential_ValidCachedAccessToken_ReturnsSnowflakeOAuthJdbcPropertiesWithoutHttp()
    {
        long nowSec = System.currentTimeMillis() / 1000;
        Map<String, String> secretMap = secretMapWithAuthCode("ac");
        secretMap.put(ACCESS_TOKEN, "cached-at");
        secretMap.put("refresh_token", "rt");
        secretMap.put(EXPIRES_IN, "3600");
        secretMap.put(FETCHED_AT, String.valueOf(nowSec - 60));

        CachableSecretsManager sm = mock(CachableSecretsManager.class);
        provider.initialize("sec", secretMap, sm);

        assertSnowflakeOAuthJdbcUserPasswordAndAuthenticator(provider.getCredential(), "cached-at");
    }

    @Test
    public void getCredential_FirstTokenExchangeViaAuthCode_ReturnsAccessTokenFromHttpResponse() throws Exception
    {
        Map<String, String> secretMap = secretMapWithAuthCode("auth-code-1");

        String tokenBody = new ObjectMapper().createObjectNode()
                .put(ACCESS_TOKEN, "new-at")
                .put("token_type", "Bearer")
                .put(EXPIRES_IN, 3600)
                .put("refresh_token", "new-rt")
                .toString();

        CachableSecretsManager sm = mock(CachableSecretsManager.class);
        SecretsManagerClient secretsClient = mock(SecretsManagerClient.class);
        when(sm.getSecretsManager()).thenReturn(secretsClient);
        provider.initialize("sec-name", secretMap, sm);

        try (MockedStatic<SnowflakeOAuthCredentialsProvider> mocked = Mockito.mockStatic(SnowflakeOAuthCredentialsProvider.class)) {
            HttpURLConnection conn = createMockHttpConnection(200, tokenBody);
            mocked.when(() -> SnowflakeOAuthCredentialsProvider.getHttpURLConnection(any(), any(), any()))
                    .thenReturn(conn);

            assertSnowflakeOAuthJdbcUserPasswordAndAuthenticator(provider.getCredential(), "new-at");
        }
    }

    private static Map<String, String> secretMapWithAuthCode(String authCode)
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(SnowflakeConstants.USERNAME, OAUTH_TEST_USER);
        secretMap.put(SnowflakeConstants.CLIENT_ID, "cid");
        secretMap.put(SnowflakeConstants.CLIENT_SECRET, "csec");
        secretMap.put(SnowflakeConstants.TOKEN_URL, OAUTH_TEST_TOKEN_URL);
        secretMap.put(SnowflakeConstants.REDIRECT_URI, OAUTH_TEST_REDIRECT);
        secretMap.put(SnowflakeConstants.AUTH_CODE, authCode);
        return secretMap;
    }

    private static Map<String, String> mapWithAuthCodeValue(String authCode)
    {
        Map<String, String> map = new HashMap<>();
        map.put(SnowflakeConstants.AUTH_CODE, authCode);
        return map;
    }

    private void assertSnowflakeOAuthJdbcUserPasswordAndAuthenticator(Credentials credentials, String expectedPassword)
    {
        assertNotNull(credentials);
        assertEquals(OAUTH_TEST_USER, credentials.getProperties().get(SnowflakeConstants.USER));
        assertEquals(expectedPassword, credentials.getProperties().get(SnowflakeConstants.PASSWORD));
        assertEquals("oauth", credentials.getProperties().get(SnowflakeConstants.AUTHENTICATOR));
    }

    private static HttpURLConnection createMockHttpConnection(int responseCode, String responseBody) throws IOException
    {
        HttpURLConnection mockConnection = mock(HttpURLConnection.class);
        when(mockConnection.getOutputStream()).thenReturn(new ByteArrayOutputStream());
        when(mockConnection.getResponseCode()).thenReturn(responseCode);
        if (responseCode >= 200 && responseCode < 300) {
            when(mockConnection.getInputStream()).thenReturn(new ByteArrayInputStream(responseBody.getBytes()));
        }
        else {
            when(mockConnection.getErrorStream()).thenReturn(new ByteArrayInputStream(responseBody.getBytes()));
        }
        return mockConnection;
    }
}
