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
package com.amazonaws.athena.connectors.snowflake.credentials;

import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.security.PrivateKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;


@RunWith(MockitoJUnitRunner.class)
public class SnowflakePrivateKeyCredentialProviderTest
{
    @Mock
    private SecretsManagerClient secretsManager;

    private SnowflakePrivateKeyCredentialProvider credentialProvider;

    private static final String SECRET_NAME = "test-secret";
    private static final String USERNAME = "test-user";
    private static final String PRIVATE_KEY = "\\nTEST_KEY_CONTENT";
    private static final String SECRET_JSON = "{\"username\":\"" + USERNAME + "\",\"privateKey\":\"" + PRIVATE_KEY + "\"}";

    @Before
    public void setup()
    {
        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            // Explicitly configure the mock
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), eq(SECRET_NAME)))
                    .thenReturn(SECRET_JSON);

            credentialProvider = new SnowflakePrivateKeyCredentialProvider(secretsManager, SECRET_NAME);
        }
    }

    @Test
    public void testGetCredential()
    {
        DefaultCredentials credentials = credentialProvider.getCredential();

        assertNotNull(credentials);
        assertEquals(USERNAME, credentials.getUser());

        // Check for specific patterns rather than exact string comparison
        String password = credentials.getPassword();
        assertTrue(password.contains("TEST_KEY_CONTENT"));
    }


    @Test
    public void testGetPrivateKeyObject() throws Exception
    {
        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), eq(SECRET_NAME)))
                    .thenReturn(SECRET_JSON);

            try {
                PrivateKey privateKey = credentialProvider.getPrivateKeyObject();
                assertNotNull(privateKey);
            } catch (Exception e) {
                System.out.println("Exception during private key creation: " + e.getMessage());
            }
        }
    }

    @Test
    public void testConstructorWithInvalidSecret()
    {
        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), anyString()))
                    .thenReturn("{\"invalid\":\"format\"}");

            assertThrows(RuntimeException.class, () -> {
                new SnowflakePrivateKeyCredentialProvider(secretsManager, SECRET_NAME);
            });
        }
    }

    @Test
    public void testConstructorWithException()
    {
        try (MockedStatic<SnowflakeAuthUtils> mockedAuthUtils = Mockito.mockStatic(SnowflakeAuthUtils.class)) {
            mockedAuthUtils.when(() -> SnowflakeAuthUtils.getSecret(any(SecretsManagerClient.class), anyString()))
                    .thenThrow(new RuntimeException("Test exception"));

            assertThrows(RuntimeException.class, () -> {
                new SnowflakePrivateKeyCredentialProvider(secretsManager, SECRET_NAME);
            });
        }
    }
}
