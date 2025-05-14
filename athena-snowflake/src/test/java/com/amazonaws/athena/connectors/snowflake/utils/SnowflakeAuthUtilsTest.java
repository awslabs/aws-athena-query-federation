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
        "HCjQ2xe8qy2axzLKm98l5KkjNx1YR7C2yyEpFJvgca1Yzr2WxJcAahe1gnazcx9a\n" +
        "ceViTjjikftrurNzmFtQmIWoLjZg3JlHBeZmEIBD3B/sGCj5uPGvkVNaDAu1AreN\n" +
        "GwafUlm8PaOnsmBh7QR/UI5OcRCIIfdHoaN7IVOPEsPqpE5Qvo1CB25L6oV+CyWJ\n" +
        "OjfiJiBdEKnhOHrxhdXxC8a/VfF9i5v3lEJin5/qgqpBdz0Zm5ryHlgwXpyG5wRY\n" +
        "1JMPHJDdFdohbr2l1Rj0wzemmo7Fq/Gw5Tz5yMKwD1D0ZBR0uTry4ZytLlzC6zkY\n" +
        "4LXNI0zNAgMBAAECggEAPi0uF/4rFGSP7xuovwIq1jmhJtFAnXDyYfWFf4rnxXm2\n" +
        "OYS/qe4+VBUSYlNm303xgQqVdgk5uVO7b8auZH0Q0MZijZ03xGvb6YG4OaL0El08\n" +
        "y2HAkgSP+Df28POGYvg6OZlZo4UIv6h2t6Ig1XEKKbnheJ+/bg1h8YCcLErjcSS+\n" +
        "0H5Dbhm1gQ3nn9+0+dS9BnC+32tCXtIeWduYxoGwYFvVwwzGR5TWTNsXrXR67tO2\n" +
        "EaoDAhbCMxnkAapaYPUcs66m2U4V7oQDBZrOnEv2HvVbjiAx1zR2H+9vuTqSKFmC\n" +
        "jnzQCnOKsbLE7AZXxkydLWAUwhyzFn+xDQDh0rv1jQKBgQDytELaa347Tu+gEsK/\n" +
        "ge/bNqAMgqmebT8pUnfmuS5h8VdVwPU+zxUYaX2u8IywbbuOBnEgAWdpxDGwnUYQ\n" +
        "mvPcPML/tOvetOcBsOGtay/QwFtmvt7V/zRw5HXdVtAkgLMlM2c2GGuWqFw7ZRHL\n" +
        "tQXqYjsAgdHsir30OfoztdR4swKBgQCpV8JwTFCVM6qfSqO104VEzusvt1bFlA4H\n" +
        "SfQqym80/A55zz6C3bYVsmM1+GaLvM5hlW6Sq1sq7NBAvnvAWLITpaizu7f7P/OC\n" +
        "D04BGBpbPXatAMnHw5zM2r0C6tqww2WPZckqFCw7W9rWjYsnFZyLj1EbdNMxN+/e\n" +
        "Rq3Lc9TkfwKBgAi1z3BnSzB1tMPZ6INW9nS3kSbhyZSV9x5Uh1kQbEm3j5rUQfjv\n" +
        "FaK6pngQyfvK9GA0evrbEgsJr37XJhyScw4EYDstEwn7FA9Lec3vetfTD3SwhO7J\n" +
        "KeijSleXNgEZXVSIc7vNRI8zm5vGFM1qwbuXquZpwk7q68ZIDmKss+NhAoGACrM1\n" +
        "4Pyhdtv94vTHZVzJJfDhIXG3NOLHBCTjHbUO0809aOr0azZxI+vSov1gFWJHtBjK\n" +
        "FNBpAUxXWE/w59Vy4xTrlPe+h0yiKUyoRB9uwuceUY4kMdAlXzhPCxkl2lduWmI3\n" +
        "FMaTiOij6jylV0HhU1wp5s857Pk42dWjc6CNCicCgYBgjc/gq1VHkNGW4gMq9hSh\n" +
        "r8fnrcvQFp10Em+Yv7MipMQ/wb5evT+2Gb4u5hCSre8S7NV9nrBEjqeBODFxZR42\n" +
        "DxkVrv9m2jFYrEqoCVXlJhZuJ8AUeje2NPqZjZgaQLLNsizjlM8dwIFTn7zYus8C\n" +
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
    public void testGetCredentialProviderWithDefault()
    {
        // This method is for backward compatibility and should return null
        com.amazonaws.athena.connector.credentials.CredentialsProvider provider = 
            SnowflakeAuthUtils.getCredentialProviderWithDefault();
        
        assertNull(provider);
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