/*-
 * #%L
 * athena-snowflake
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

package com.amazonaws.athena.connectors.snowflake.utils;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeAuthUtilsTest
{
    @Mock
    private SecretsManagerClient secretsManager;

    private static final String SECRET_NAME = "test-secret";
    private static final String SECRET_VALUE = "{\"username\":\"test-user\",\"password\":\"test-password\"}";
    private static final String PRIVATE_KEY_SECRET_VALUE = "{\"username\":\"test-user\",\"privateKey\":\"test-private-key\"}";

    @Before
    public void setup()
    {
        GetSecretValueResponse response = GetSecretValueResponse.builder()
                .secretString(SECRET_VALUE)
                .build();
        
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(response);
    }

    @Test
    public void testGetSecret()
    {
        String secretValue = SnowflakeAuthUtils.getSecret(secretsManager, SECRET_NAME);
        assertEquals(SECRET_VALUE, secretValue);
    }

    @Test
    public void testExtractParameterFromConnectionString()
    {
        assertEquals("my-value", SnowflakeAuthUtils.extractParameterFromConnectionString("snowflake://jdbc:snowflake://host/?param=my-value", "param"));
        assertEquals("my-value", SnowflakeAuthUtils.extractParameterFromConnectionString("snowflake://jdbc:snowflake://host/?other=value&param=my-value", "param"));
        assertNull(SnowflakeAuthUtils.extractParameterFromConnectionString("snowflake://jdbc:snowflake://host/?other=value", "param"));
        assertNull(SnowflakeAuthUtils.extractParameterFromConnectionString(null, "param"));
        assertNull(SnowflakeAuthUtils.extractParameterFromConnectionString("snowflake://jdbc:snowflake://host/?param=my-value", null));
    }

    @Test
    public void testExtractParametersFromConnectionString()
    {
        String connectionString = "snowflake://jdbc:snowflake://host/?secret=my-secret&warehouse=my-wh&schema=my-schema";
        
        Map<String, String> params = SnowflakeAuthUtils.extractParametersFromConnectionString(connectionString);
        
        assertEquals(3, params.size());
        assertEquals("my-secret", params.get("secret"));
        assertEquals("my-wh", params.get("warehouse"));
        assertEquals("my-schema", params.get("schema"));

        connectionString = "snowflake://jdbc:snowflake://host/?${my-secret}";
        params = SnowflakeAuthUtils.extractParametersFromConnectionString(connectionString);
        
        assertEquals(1, params.size());
        assertEquals("my-secret", params.get("secret"));

        params = SnowflakeAuthUtils.extractParametersFromConnectionString(null);
        assertTrue(params.isEmpty());
    }

    @Test
    public void testGetSecretName()
    {
        String connectionString = "snowflake://jdbc:snowflake://host/?secret=test-secret";
        assertEquals("test-secret", SnowflakeAuthUtils.getSecretName(connectionString));

        connectionString = "snowflake://jdbc:snowflake://host/?${test-secret-placeholder}";
        assertEquals("test-secret-placeholder", SnowflakeAuthUtils.getSecretName(connectionString));

        assertNull(SnowflakeAuthUtils.getSecretName(null));
    }

    @Test
    public void testCreateCredentialProvider() throws Exception
    {
        // Test with password secret
        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), eq(SECRET_NAME)))
                    .thenReturn("{\"username\":\"test-user\",\"password\":\"test-password\"}");

            mockedAuthUtils.when(SnowflakeAuthUtils::getSecretsManager)
                    .thenReturn(secretsManager);

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createCredentialProvider(anyString()))
                    .thenCallRealMethod();

            CredentialsProvider provider = SnowflakeAuthUtils.createCredentialProvider(SECRET_NAME);

            assertNotNull(provider);
            assertTrue(provider instanceof DefaultCredentialsProvider);
        }

        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), eq(SECRET_NAME)))
                    .thenReturn("{\"username\":\"test-user\",\"privateKey\":\"test-private-key\"}");

            mockedAuthUtils.when(SnowflakeAuthUtils::getSecretsManager)
                    .thenReturn(secretsManager);

            mockedAuthUtils.when(() -> SnowflakeAuthUtils.createCredentialProvider(anyString()))
                    .thenCallRealMethod();

            CredentialsProvider provider = SnowflakeAuthUtils.createCredentialProvider(SECRET_NAME);

            assertNotNull(provider);
            assertTrue(provider instanceof SnowflakePrivateKeyCredentialProvider);
        }
    }
}
