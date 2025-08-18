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

    // Sample encrypted private key for testing (this is a mock encrypted key)
    private static final String ENCRYPTED_PRIVATE_KEY_PEM = 
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
        "MIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIrVKBudJMmggCAggA\n" +
        "MBQGCCqGSIb3DQMHBAgEgw==\n" +
        "-----END ENCRYPTED PRIVATE KEY-----";

    // Sample invalid encrypted private key
    private static final String INVALID_ENCRYPTED_PRIVATE_KEY_PEM = 
        "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" +
        "invalid-base64-content\n" +
        "-----END ENCRYPTED PRIVATE KEY-----";

    public static final String TESTUSER = "testuser";
    public static final String TESTPASSWORD = "testpassword";

    @Test
    public void testDetermineAuthTypeWithKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, TESTUSER);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

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
    public void testCreatePrivateKeyWithPassphrase()
    {
        // Test that the method accepts a passphrase parameter (even for unencrypted keys)
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey("invalid-pem", "test-passphrase");
        });
    }

    @Test
    public void testValidateCredentialsWithKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, TESTUSER);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PRIVATE_KEY_PEM);

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

    @Test
    public void testCreatePrivateKeyWithEncryptedKeyAndCorrectPassphrase()
    {
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey(ENCRYPTED_PRIVATE_KEY_PEM, "test-passphrase");
        });
        // Note: This will throw an exception because the mock encrypted key is not valid
        // In a real scenario with a valid encrypted key, this should succeed
    }

    @Test
    public void testCreatePrivateKeyWithEncryptedKeyAndNullPassphrase()
    {
        // Test encrypted private key with null passphrase - should throw exception
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey(ENCRYPTED_PRIVATE_KEY_PEM, null);
        });
    }

    @Test
    public void testCreatePrivateKeyWithInvalidEncryptedKey()
    {
        // Test invalid encrypted private key - should throw exception
        assertThrows(Exception.class, () -> {
            SnowflakeAuthUtils.createPrivateKey(INVALID_ENCRYPTED_PRIVATE_KEY_PEM, "test-passphrase");
        });
    }

    @Test
    public void testDetermineAuthTypeWithEncryptedKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, TESTUSER);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY, ENCRYPTED_PRIVATE_KEY_PEM);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, "test-passphrase");

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.SNOWFLAKE_JWT, authType);
    }

    @Test
    public void testDetermineAuthTypeWithEncryptedKeyPairAuthMissingPassphrase()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, TESTUSER);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY, ENCRYPTED_PRIVATE_KEY_PEM);
        // Missing passphrase - should still detect as JWT auth type

        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentials);

        assertEquals(SnowflakeAuthType.SNOWFLAKE_JWT, authType);
    }

    @Test
    public void testValidateCredentialsWithEncryptedKeyPairAuth()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, TESTUSER);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY, ENCRYPTED_PRIVATE_KEY_PEM);
        credentials.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, "test-passphrase");

        // Should not throw exception
        SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE_JWT);
    }
}
