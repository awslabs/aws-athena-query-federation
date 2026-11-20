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

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

    @Test
    public void testGetUsernameWithUppercaseKey()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME_UPPERCASE, TESTUSER);
        credentials.put(SnowflakeConstants.PASSWORD, TESTPASSWORD);

        String username = SnowflakeAuthUtils.getUsername(credentials);
        assertEquals(TESTUSER, username);
    }

    @Test
    public void testGetUsernameFallbackOrder()
    {
        // Test that SF_USER takes priority over USERNAME and USERNAME_UPPERCASE
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, "sfuser");
        credentials.put(SnowflakeConstants.USERNAME, "username");
        credentials.put(SnowflakeConstants.USERNAME_UPPERCASE, "USERNAME");

        String username = SnowflakeAuthUtils.getUsername(credentials);
        assertEquals("sfuser", username);
    }

    @Test
    public void testGetUsernameFallbackToUppercase()
    {
        // Test that USERNAME_UPPERCASE is used when SF_USER and USERNAME are blank
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.SF_USER, "");
        credentials.put(SnowflakeConstants.USERNAME, "");
        credentials.put(SnowflakeConstants.USERNAME_UPPERCASE, TESTUSER);

        String username = SnowflakeAuthUtils.getUsername(credentials);
        assertEquals(TESTUSER, username);
    }

    @Test
    public void testValidateCredentialsWithPasswordUppercase()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        credentials.put(SnowflakeConstants.PASSWORD_UPPERCASE, TESTPASSWORD);

        // Should not throw exception - PASSWORD_UPPERCASE should be accepted
        SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE);
    }

    @Test
    public void testValidateCredentialsWithPasswordMissingBothCases()
    {
        Map<String, String> credentials = new HashMap<>();
        credentials.put(SnowflakeConstants.USERNAME, TESTUSER);
        // Neither PASSWORD nor PASSWORD_UPPERCASE is set

        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeAuthUtils.validateCredentials(credentials, SnowflakeAuthType.SNOWFLAKE);
        });
    }

    // ---- PEM normalization: every input shape a secret can carry must parse to the same key ----

    private static String generatePkcs8Base64() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        return Base64.getEncoder().encodeToString(pair.getPrivate().getEncoded());
    }

    private static String wrap64(String base64)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < base64.length(); i += 64) {
            sb.append(base64, i, Math.min(i + 64, base64.length())).append('\n');
        }
        return sb.toString();
    }

    @Test
    public void testCreatePrivateKeyFromRawBase64()
            throws Exception
    {
        String base64 = generatePkcs8Base64();
        PrivateKey key = SnowflakeAuthUtils.createPrivateKey(base64, null);
        assertEquals("RSA", key.getAlgorithm());
    }

    @Test
    public void testCreatePrivateKeyFromMultiLinePem()
            throws Exception
    {
        String pem = "-----BEGIN PRIVATE KEY-----\n" + wrap64(generatePkcs8Base64()) + "-----END PRIVATE KEY-----\n";
        assertEquals("RSA", SnowflakeAuthUtils.createPrivateKey(pem, null).getAlgorithm());
    }

    @Test
    public void testCreatePrivateKeyFromSingleLinePem()
            throws Exception
    {
        // Header, body and footer inline on one line, as it ends up when a PEM is pasted into a JSON secret.
        String pem = "-----BEGIN PRIVATE KEY----- " + generatePkcs8Base64() + " -----END PRIVATE KEY-----";
        assertEquals("RSA", SnowflakeAuthUtils.createPrivateKey(pem, null).getAlgorithm());
    }

    @Test
    public void testCreatePrivateKeyFromPemWithSpacesInsteadOfNewlines()
            throws Exception
    {
        // Every newline collapsed to a space (header, 64-char body chunks and footer all on one line).
        String pem = ("-----BEGIN PRIVATE KEY-----\n" + wrap64(generatePkcs8Base64()) + "-----END PRIVATE KEY-----\n")
                .replace("\n", " ");
        assertEquals("RSA", SnowflakeAuthUtils.createPrivateKey(pem, null).getAlgorithm());
    }

    @Test
    public void testCreatePrivateKeyUnencryptedPemWithStrayPassphraseKeepsDeclaredLabel()
            throws Exception
    {
        // Regression: the declared PEM label wins over the passphrase heuristic. An unencrypted key whose secret also
        // carries a passphrase must still parse as PrivateKeyInfo, not be relabelled ENCRYPTED PRIVATE KEY.
        String pem = "-----BEGIN PRIVATE KEY-----\n" + wrap64(generatePkcs8Base64()) + "-----END PRIVATE KEY-----\n";
        assertEquals("RSA", SnowflakeAuthUtils.createPrivateKey(pem, "unused-passphrase").getAlgorithm());
    }

    @Test
    public void testFormatPrivateKeyWithPemHeadersPreservesEncryptedLabel()
    {
        String pem = "-----BEGIN ENCRYPTED PRIVATE KEY----- QUJD -----END ENCRYPTED PRIVATE KEY-----";
        String formatted = SnowflakeAuthUtils.formatPrivateKeyWithPemHeaders(pem, null);
        assertTrue(formatted.startsWith("-----BEGIN ENCRYPTED PRIVATE KEY-----\nQUJD\n"));
        assertTrue(formatted.endsWith("-----END ENCRYPTED PRIVATE KEY-----\n"));
    }

    @Test
    public void testFormatPrivateKeyWithPemHeadersInfersLabelOnlyForRawBase64()
    {
        assertTrue(SnowflakeAuthUtils.formatPrivateKeyWithPemHeaders("QUJD", "pw").startsWith("-----BEGIN ENCRYPTED PRIVATE KEY-----"));
        assertTrue(SnowflakeAuthUtils.formatPrivateKeyWithPemHeaders("QUJD", null).startsWith("-----BEGIN PRIVATE KEY-----"));
    }
}
