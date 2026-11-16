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
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.USER;

/**
 * Credentials provider for Snowflake key-pair (JWT) authentication. Used when the secret is not OAuth or username/password only.
 */
public class SnowflakeKeyPairCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeKeyPairCredentialsProvider.class);

    private final Map<String, String> credentialMap;

    public SnowflakeKeyPairCredentialsProvider(Map<String, String> secretMap)
    {
        Map<String, String> m = new HashMap<>();
        String username = SnowflakeAuthUtils.getUsername(secretMap);
        m.put(USER, username);
        String pem = secretMap.get(SnowflakeConstants.PEM_PRIVATE_KEY);
        m.put(SnowflakeConstants.PEM_PRIVATE_KEY, pem);
        String passphrase = secretMap.get(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE);
        if (passphrase != null) {
            m.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, passphrase);
        }
        this.credentialMap = Collections.unmodifiableMap(m);
        LOGGER.debug("Using key-pair authentication for user: {}", username);
    }

    @Override
    public Credentials getCredential()
    {
        Map<String, String> props = credentialMap;
        return () -> props;
    }
}
