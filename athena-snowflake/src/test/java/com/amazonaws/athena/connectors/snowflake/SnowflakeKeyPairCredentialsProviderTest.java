/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeKeyPairCredentialsProviderTest
{
    private static final String JWT_TEST_USER = "jwt-user";

    private static final String VALID_PEM =
            "-----BEGIN PRIVATE KEY-----\n"
                    + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCgjDO7+xGLs5bw\n"
                    + "DxkVrv9m2jFYrEqoCVXlJhZuJ8AUeje2NPqZjZgaQLLNsizjlM8dwIFTn7zYus8C\n"
                    + "0/EKcWRPSoI9PsbgwvFIyQ==\n"
                    + "-----END PRIVATE KEY-----";

    @Test
    public void getCredential_PemPrivateKeyInSecret_ExposesUserAndPemInProperties()
    {
        Map<String, String> secret = newJwtUserSecretMap();
        secret.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PEM);

        SnowflakeKeyPairCredentialsProvider provider = new SnowflakeKeyPairCredentialsProvider(secret);
        Credentials credentials = provider.getCredential();
        Map<String, String> props = credentials.getProperties();

        assertEquals(JWT_TEST_USER, props.get(CredentialsConstants.USER));
        assertEquals(VALID_PEM, props.get(SnowflakeConstants.PEM_PRIVATE_KEY));
    }

    @Test
    public void getCredential_EncryptedKeyWithPassphrase_IncludesPassphraseInCredentialMap()
    {
        Map<String, String> secret = newJwtUserSecretMap();
        secret.put(SnowflakeConstants.PEM_PRIVATE_KEY, "-----BEGIN ENCRYPTED PRIVATE KEY-----\nX\n-----END ENCRYPTED PRIVATE KEY-----");
        secret.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, "p4ss");

        SnowflakeKeyPairCredentialsProvider provider = new SnowflakeKeyPairCredentialsProvider(secret);
        Map<String, String> map = provider.getCredentialMap();

        assertEquals("p4ss", map.get(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE));
    }

    @Test
    public void constructor_MissingUsername_ThrowsIllegalArgumentException()
    {
        Map<String, String> secret = new HashMap<>();
        secret.put(SnowflakeConstants.PEM_PRIVATE_KEY, VALID_PEM);

        assertThrows(IllegalArgumentException.class, () -> new SnowflakeKeyPairCredentialsProvider(secret));
    }

    private static Map<String, String> newJwtUserSecretMap()
    {
        Map<String, String> secret = new HashMap<>();
        secret.put("username", JWT_TEST_USER);
        return secret;
    }
}
