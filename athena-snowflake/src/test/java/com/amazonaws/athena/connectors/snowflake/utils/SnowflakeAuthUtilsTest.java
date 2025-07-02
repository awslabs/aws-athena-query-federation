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

import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeAuthUtilsTest
{
    // Valid 2048-bit RSA private key for testing
    private static final String VALID_PRIVATE_KEY_PEM = 
        "-----BEGIN PRIVATE KEY-----\n" +
        "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCgjDO7+xGLs5bw\n" +
        "0/EKcWRPSoI9PsbgwvFIyQ==\n" +
        "-----END PRIVATE KEY-----";
    public static final String TESTUSER = "testuser";
    public static final String TESTPASSWORD = "testpassword";

    @Test
    public void testDetermineAuthTypeWithKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.SNOWFLAKE_JWT, authType);
    }

    @Test
    public void testDetermineAuthTypeWithOAuthAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.AUTH_CODE, "test-auth-code");

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.OAUTH, authType);
    }

    @Test
    public void testDetermineAuthTypeWithPasswordAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PASSWORD, TESTPASSWORD);

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.SNOWFLAKE, authType);
    }

    @Test
    public void testDetermineAuthTypeWithNullCredentials()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.determineAuthType(null);
        });
    }

    @Test
    public void testDetermineAuthTypeWithEmptyCredentials()
    {
        Map<String, String> credentials = new HashMap<>();
        
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.determineAuthType(credentials);
        });
    }

    @Test
    public void testDetermineAuthTypeWithEmptyPrivateKey()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PRIVATE_KEY, "");
        credentials.put(SnowflakeConstants.PASSWORD, TESTPASSWORD); // Add password for fallback

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.SNOWFLAKE, authType);
    }

    @Test
    public void testCreatePrivateKeyWithInvalidPEM()
    {
        String invalidPEM = "-----BEGIN PRIVATE KEY-----\ninvalid-key-content\n-----END PRIVATE KEY-----";

        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey(invalidPEM);
        });
    }

    @Test
    public void testCreatePrivateKeyWithNullPEM()
    {
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey(null);
        });
    }

    @Test
    public void testCreatePrivateKeyWithEmptyPEM()
    {
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey("");
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        // Should not throw exception
        SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
    }

    @Test
    public void testValidateCredentialsWithOAuthAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.AUTH_CODE, "test-auth-code");

        // Should not throw exception
        SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.OAUTH);
    }

    @Test
    public void testValidateCredentialsWithPasswordAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PASSWORD, TESTPASSWORD);

        // Should not throw exception
        SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE);
    }

    @Test
    public void testValidateCredentialsWithNullCredentials()
    {
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(null, SnowflakeAuthType.SNOWFLAKE_JWT);
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuthMissingUsername()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuthEmptyUsername()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, "");
        credentials.put(SnowflakeConstants.PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuthMissingPrivateKey()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuthEmptyPrivateKey()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PRIVATE_KEY, "");

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
        });
    }

    @Test
    public void testValidateCredentialsWithOAuthAuthMissingUsername()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.AUTH_CODE, "test-auth-code");

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.OAUTH);
        });
    }

    @Test
    public void testSnowflakeAuthTypeFromString()
    {
        assertEquals(SnowflakeAuthType.SNOWFLAKE, SnowflakeAuthType.fromString("snowflake"));
        assertEquals(SnowflakeAuthType.SNOWFLAKE_JWT, SnowflakeAuthType.fromString("snowflake_jwt"));
        assertEquals(SnowflakeAuthType.OAUTH, SnowflakeAuthType.fromString("oauth"));
    }

    @Test
    public void testSnowflakeAuthTypeGetValue()
    {
        assertEquals("snowflake", SnowflakeAuthType.SNOWFLAKE.getValue());
        assertEquals("snowflake_jwt", SnowflakeAuthType.SNOWFLAKE_JWT.getValue());
        assertEquals("oauth", SnowflakeAuthType.OAUTH.getValue());
    }

    @Test
    public void testSnowflakeAuthTypeToString()
    {
        assertEquals("snowflake", SnowflakeAuthType.SNOWFLAKE.toString());
        assertEquals("snowflake_jwt", SnowflakeAuthType.SNOWFLAKE_JWT.toString());
        assertEquals("oauth", SnowflakeAuthType.OAUTH.toString());
    }
} 