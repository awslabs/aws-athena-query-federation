/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeAuthUtilsTest
{
    @Mock
    private SecretsManagerClient secretsManager;

    private static final String SECRET_NAME = "test-secret";
    private static final String SECRET_VALUE = "{\"username\":\"test-user\",\"password\":\"test-password\"}";
    private static final String PRIVATE_KEY_SECRET_VALUE = "{\"username\":\"test-user\",\"private_key\":\"test-private-key\"}";

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
    public void testGetCredentialProviderWithPasswordAuth()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(SnowflakeConstants.AUTH_TYPE, SnowflakeConstants.AUTH_TYPE_PASSWORD);
        
        CredentialsProvider provider = SnowflakeAuthUtils.getCredentialProvider(secretsManager, SECRET_NAME, configOptions);
        
        assertTrue(provider instanceof DefaultCredentialsProvider);
    }

    @Test
    public void testGetCredentialProviderWithPrivateKeyAuth()
    {
        GetSecretValueResponse privateKeyResponse = GetSecretValueResponse.builder()
                .secretString(PRIVATE_KEY_SECRET_VALUE)
                .build();
        
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(privateKeyResponse);
        
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(SnowflakeConstants.AUTH_TYPE, SnowflakeConstants.AUTH_TYPE_PRIVATE_KEY);
        
        CredentialsProvider provider = SnowflakeAuthUtils.getCredentialProvider(secretsManager, SECRET_NAME, configOptions);
        
        assertTrue(provider instanceof SnowflakePrivateKeyCredentialProvider);
    }

    @Test
    public void testExtractSecretNameFromConnectionString()
    {
        assertEquals("my-secret", SnowflakeAuthUtils.extractSecretNameFromConnectionString("snowflake://jdbc:snowflake://host/?secret_name=my-secret"));
        assertEquals("my-secret", SnowflakeAuthUtils.extractSecretNameFromConnectionString("snowflake://jdbc:snowflake://host/?param=value&secret_name=my-secret&other=value"));
        assertEquals("my-secret", SnowflakeAuthUtils.extractSecretNameFromConnectionString("snowflake://jdbc:snowflake://host/?${my-secret}"));
        assertNull(SnowflakeAuthUtils.extractSecretNameFromConnectionString("snowflake://jdbc:snowflake://host/?param=value"));
        assertNull(SnowflakeAuthUtils.extractSecretNameFromConnectionString(null));
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
        String connectionString = "snowflake://jdbc:snowflake://host/?secret_name=my-secret&auth_type=private_key&warehouse=my-wh&schema=my-schema";
        
        Map<String, String> params = SnowflakeAuthUtils.extractParametersFromConnectionString(connectionString);
        
        assertEquals(4, params.size());
        assertEquals("my-secret", params.get("secret_name"));
        assertEquals("private_key", params.get("auth_type"));
        assertEquals("my-wh", params.get("warehouse"));
        assertEquals("my-schema", params.get("schema"));

        connectionString = "snowflake://jdbc:snowflake://host/?${my-secret}&auth_type=private_key";
        params = SnowflakeAuthUtils.extractParametersFromConnectionString(connectionString);
        
        assertEquals(2, params.size());
        assertEquals("my-secret", params.get("secret_name"));
        assertEquals("private_key", params.get("auth_type"));

        params = SnowflakeAuthUtils.extractParametersFromConnectionString(null);
        assertTrue(params.isEmpty());
    }

    @Test
    public void testUpdateConfigOptionsFromConnectionString()
    {
        String connectionString = "snowflake://jdbc:snowflake://host/?secret_name=my-secret&auth_type=private_key";
        Map<String, String> configOptions = new HashMap<>();
        
        boolean result = SnowflakeAuthUtils.updateConfigOptionsFromConnectionString(connectionString, configOptions);
        
        assertTrue(result);
        assertEquals(2, configOptions.size());
        assertEquals("my-secret", configOptions.get("secret_name"));
        assertEquals("private_key", configOptions.get("auth_type"));

        Map<String, String> unmodifiableMap = Collections.unmodifiableMap(new HashMap<>());
        result = SnowflakeAuthUtils.updateConfigOptionsFromConnectionString(connectionString, unmodifiableMap);
        
        assertFalse(result);
        assertEquals(0, unmodifiableMap.size());

        result = SnowflakeAuthUtils.updateConfigOptionsFromConnectionString(null, configOptions);
        assertFalse(result);
    }

    @Test
    public void testGetSecretNameFromSources()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("secret_name", "config-secret");
        
        String secretName = SnowflakeAuthUtils.getSecretNameFromSources(configOptions, null);
        assertEquals("config-secret", secretName);

        CredentialsProvider parentProvider = new DefaultCredentialsProvider(SECRET_VALUE);
        secretName = SnowflakeAuthUtils.getSecretNameFromSources(new HashMap<>(), parentProvider);
        assertNull(secretName);
    }

    @Test
    public void testUpdateConfigOptionsFromSecret()
    {
        GetSecretValueResponse authTypeResponse = GetSecretValueResponse.builder()
                .secretString("{\"username\":\"test-user\",\"password\":\"test-password\",\"auth_type\":\"private_key\"}")
                .build();
        
        when(secretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenReturn(authTypeResponse);
        
        Map<String, String> configOptions = new HashMap<>();
        
        SnowflakeAuthUtils.updateConfigOptionsFromSecret(secretsManager, SECRET_NAME, configOptions);
        
        assertEquals(1, configOptions.size());
        assertEquals("private_key", configOptions.get(SnowflakeConstants.AUTH_TYPE));
    }
}
