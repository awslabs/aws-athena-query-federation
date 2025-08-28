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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
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

import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_ID;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.CLIENT_SECRET;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.PASSWORD;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.USERNAME;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2OAuthCredentialsProviderTest.SECRET_NAME;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2OAuthCredentialsProviderTest.TENANT_ID;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2OAuthCredentialsProviderTest.TEST_CLIENT_ID;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2OAuthCredentialsProviderTest.TEST_CLIENT_SECRET;
import static com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2OAuthCredentialsProviderTest.TEST_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataLakeGen2CredentialProviderUtilsTest
{
    @Mock
    private SecretsManagerClient secretsManager;

    private CachableSecretsManager cachableSecretsManager;

    @Before
    public void setup()
    {
        cachableSecretsManager = new CachableSecretsManager(secretsManager);
    }

    @Test
    public void testGetCredentialProvider_whenOAuthConfigured() throws JsonProcessingException
    {
        // Mock OAuth secret response
        String oauthSecret = new ObjectMapper().writeValueAsString(Map.of(
                CLIENT_ID, TEST_CLIENT_ID,
                CLIENT_SECRET, TEST_CLIENT_SECRET,
                TENANT_ID, TEST_TENANT_ID
        ));
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString(oauthSecret).build()
        );

        CredentialsProvider provider = DataLakeGen2CredentialProviderUtils.getCredentialProvider(SECRET_NAME, cachableSecretsManager);
        assertTrue(provider instanceof DataLakeGen2OAuthCredentialsProvider);
    }

    @Test
    public void testGetCredentialProvider_whenUsernamePasswordConfigured() throws JsonProcessingException
    {
        // Mock username/password secret response
        String standardSecret = new ObjectMapper().writeValueAsString(Map.of(
                USERNAME, "test-user",
                PASSWORD, "test-password"
        ));
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString(standardSecret).build()
        );

        CredentialsProvider provider = DataLakeGen2CredentialProviderUtils.getCredentialProvider(SECRET_NAME, cachableSecretsManager);
        assertTrue(provider instanceof DefaultCredentialsProvider);
    }

    @Test
    public void testGetCredentialProvider_whenNoSecret()
    {
        CredentialsProvider provider = DataLakeGen2CredentialProviderUtils.getCredentialProvider("", cachableSecretsManager);
        assertNull(provider);
    }

    @Test
    public void testGetCredentialProvider_whenInvalidJson_throwsException()
    {
        // Mock invalid JSON response
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
            GetSecretValueResponse.builder().secretString("invalid-json{").build()
        );

        try {
            DataLakeGen2CredentialProviderUtils.getCredentialProvider(SECRET_NAME, cachableSecretsManager);
            fail("Expected AthenaConnectorException");
        }
        catch (AthenaConnectorException e) {
            assertEquals(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString(), 
                e.getErrorDetails().errorCode());
            assertTrue(e.getMessage().contains("Could not deserialize RDS credentials into HashMap"));
        }
    }
}
