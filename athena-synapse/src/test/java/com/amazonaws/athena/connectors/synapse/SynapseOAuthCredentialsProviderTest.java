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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.http.HttpClient;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class SynapseOAuthCredentialsProviderTest
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

    private SynapseOAuthCredentialsProvider credentialsProvider;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        CachableSecretsManager cachableSecretsManager = new CachableSecretsManager(secretsManagerClient);
        credentialsProvider = new SynapseOAuthCredentialsProvider(httpClient);
        credentialsProvider.initialize(SECRET_NAME, new HashMap<>(), cachableSecretsManager);
    }

    @Test
    public void testIsOAuthConfigured_WithValidConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CredentialsConstants.CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CredentialsConstants.CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        assertTrue(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testIsOAuthConfigured_WithMissingConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CredentialsConstants.CLIENT_ID, TEST_CLIENT_ID);
        // Missing client_secret and tenant_id

        assertFalse(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testIsOAuthConfigured_WithEmptyConfig()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CredentialsConstants.CLIENT_ID, "");
        secretMap.put(CredentialsConstants.CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        assertFalse(credentialsProvider.isOAuthConfigured(secretMap));
    }

    @Test
    public void testBuildTokenRequest()
    {
        Map<String, String> secretMap = new HashMap<>();
        secretMap.put(CredentialsConstants.CLIENT_ID, TEST_CLIENT_ID);
        secretMap.put(CredentialsConstants.CLIENT_SECRET, TEST_CLIENT_SECRET);
        secretMap.put(TENANT_ID, TEST_TENANT_ID);

        var request = credentialsProvider.buildTokenRequest(secretMap);

        assertTrue(request.uri().toString().contains("login.microsoftonline.com"));
        assertTrue(request.uri().toString().contains(TEST_TENANT_ID));
        assertTrue(request.headers().firstValue("Content-Type").orElse("").contains("application/x-www-form-urlencoded"));
    }
}
