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

package com.amazonaws.athena.connectors.snowflake.credentials;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Map;

/**
 * Provides credentials for Snowflake using private key authentication.
 */
public class SnowflakePrivateKeyCredentialProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakePrivateKeyCredentialProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DefaultCredentials credentials;

    /**
     * Creates a new SnowflakePrivateKeyCredentialProvider.
     *
     * @param secretsManager SecretsManager client
     * @param secretName Secret name
     */
    public SnowflakePrivateKeyCredentialProvider(SecretsManagerClient secretsManager, String secretName)
    {
        LOGGER.debug("SnowflakePrivateKeyCredentialProvider is called");
        try {
            String secretString = SnowflakeAuthUtils.getSecret(secretsManager, secretName);
            Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);

            String username = secretMap.get("username");
            String privateKey = secretMap.get("private_key");

            if (username == null || privateKey == null) {
                throw new RuntimeException("Secret must contain 'username' and 'private_key' fields");
            }

            this.credentials = new DefaultCredentials(username, privateKey);
        }
        catch (Exception e) {
            LOGGER.error("Error retrieving credentials from Secrets Manager", e);
            throw new RuntimeException("Error retrieving credentials from Secrets Manager", e);
        }
    }

    @Override
    public DefaultCredentials getCredential()
    {
        return credentials;
    }

    /**
     * Generates a PrivateKey object from the private key string.
     *
     * @return PrivateKey object
     */
    public PrivateKey getPrivateKeyObject()
    {
        try {
            // Add Bouncy Castle provider
            Security.addProvider(new BouncyCastleProvider());
            String privateKeyPEM = credentials.getPassword();
            LOGGER.debug("Converting private key string to PrivateKey object");
            
            // Replace literal \n with actual newlines in the string
            if (!privateKeyPEM.contains("\r\n") && !privateKeyPEM.contains("\n")) {
                LOGGER.info("Replacing literal \\n with actual newlines in private key");
                privateKeyPEM = privateKeyPEM.replace("\\n", "\n");
            }

            // Read PEM object from private key string
            PEMParser pemParser = new PEMParser(new StringReader(privateKeyPEM));
            Object pemObject = pemParser.readObject();
            
            if (pemObject == null) {
                pemParser.close();
                LOGGER.error("Failed to parse private key: PEM object is null");
                throw new RuntimeException("Failed to parse private key: PEM object is null");
            }

            // Only support unencrypted private keys
            if (pemObject instanceof PrivateKeyInfo) {
                // Unencrypted private key
                LOGGER.debug("Private key is not encrypted (PrivateKeyInfo)");
                PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) pemObject;
                pemParser.close();

                // Convert PEM to PrivateKey object
                JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
                PrivateKey privateKey = converter.getPrivateKey(privateKeyInfo);

                LOGGER.debug("Successfully converted private key string to PrivateKey object");
                return privateKey;
            }
            else {
                pemParser.close();
                // Don't support encrypted private keys or other formats
                LOGGER.error("Unsupported private key format: {}. Only unencrypted private keys are supported.",
                        pemObject.getClass().getName());
                throw new RuntimeException("Unsupported private key format. Only unencrypted private keys (PrivateKeyInfo) are supported, but got: "
                        + pemObject.getClass().getName());
            }
        }
        catch (Exception e) {
            LOGGER.error("Error converting private key string to PrivateKey object", e);
            throw new RuntimeException("Failed to convert private key: " + e.getMessage(), e);
        }
    }
}
