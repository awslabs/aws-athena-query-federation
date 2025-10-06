/*-
 * #%L
 * athena-federation-sdk
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
import com.fasterxml.jackson.core.JsonProcessingException;
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

import java.net.http.HttpRequest;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.PASSWORD;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CredentialsProviderFactoryTest
{
    private static final String SECRET_NAME = "test-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_USERNAME = "test-user";
    private static final String TEST_PASSWORD = "test-password";

    @Mock
    private SecretsManagerClient secretsManager;

    private CachableSecretsManager cachableSecretsManager;

    @Before
    public void setup()
    {
        cachableSecretsManager = new CachableSecretsManager(secretsManager);
    }

    // Test OAuth provider class for testing
    public static class TestOAuthProvider extends OAuthCredentialsProvider
    {
        @Override
        protected boolean isOAuthConfigured(Map<String, String> secretMap)
        {
            return secretMap.containsKey(CLIENT_ID) && secretMap.containsKey(CLIENT_SECRET);
        }

        @Override
        protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
        {
            return HttpRequest.newBuilder().uri(java.net.URI.create("http://test")).build();
        }
    }

    @Test
    public void testCreateCredentialProvider_whenOAuthConfigured() throws JsonProcessingException
    {
        // Mock OAuth secret response
        String oauthSecret = new ObjectMapper().writeValueAsString(Map.of(
                CLIENT_ID, TEST_CLIENT_ID,
                CLIENT_SECRET, TEST_CLIENT_SECRET
        ));
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString(oauthSecret).build()
        );

        CredentialsProvider provider = CredentialsProviderFactory.createCredentialProvider(
                SECRET_NAME, 
                cachableSecretsManager,
                new TestOAuthProvider()
        );
        assertTrue(provider instanceof TestOAuthProvider);
    }

    @Test
    public void testCreateCredentialProvider_whenUsernamePasswordConfigured() throws JsonProcessingException
    {
        // Mock username/password secret response
        String standardSecret = new ObjectMapper().writeValueAsString(Map.of(
                USERNAME, TEST_USERNAME,
                PASSWORD, TEST_PASSWORD
        ));
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString(standardSecret).build()
        );

        CredentialsProvider provider = CredentialsProviderFactory.createCredentialProvider(
                SECRET_NAME,
                cachableSecretsManager,
                new TestOAuthProvider()
        );
        assertTrue(provider instanceof DefaultCredentialsProvider);
    }

    @Test
    public void testCreateCredentialProvider_whenNoSecret()
    {
        CredentialsProvider provider = CredentialsProviderFactory.createCredentialProvider(
                "",
                cachableSecretsManager,
                new TestOAuthProvider()
        );
        assertNull(provider);
    }

    @Test
    public void testCreateCredentialProvider_whenInvalidJson_throwsException()
    {
        // Mock invalid JSON response
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString("invalid-json{").build()
        );

        try {
            CredentialsProviderFactory.createCredentialProvider(
                    SECRET_NAME,
                    cachableSecretsManager,
                    new TestOAuthProvider()
            );
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Could not deserialize credentials into HashMap"));
        }
    }

    @Test
    public void testCreateCredentialProvider_whenInvalidOAuthClass_throwsException() throws JsonProcessingException
    {
        // Create a class that doesn't properly implement the required methods
        class InvalidOAuthProvider extends OAuthCredentialsProvider
        {
            @Override
            protected boolean isOAuthConfigured(Map<String, String> secretMap)
            {
                throw new RuntimeException("Test exception");
            }

            @Override
            protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
            {
                return null;
            }
        }

        String oauthSecret = new ObjectMapper().writeValueAsString(Map.of(
                CLIENT_ID, TEST_CLIENT_ID,
                CLIENT_SECRET, TEST_CLIENT_SECRET
        ));
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
                GetSecretValueResponse.builder().secretString(oauthSecret).build()
        );

        try {
            CredentialsProviderFactory.createCredentialProvider(
                    SECRET_NAME,
                    cachableSecretsManager,
                    new InvalidOAuthProvider()
            );
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(),
                    e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Failed to create OAuth provider"));
        }
    }
}
