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
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Map;

/**
 * Utility class for Snowflake authentication methods.
 */
public class SnowflakeAuthUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeAuthUtils.class);

    private SnowflakeAuthUtils()
    {
    }

    /**
     * Determines the authentication type based on the provided credentials.
     * 
     * @param credentials The credentials map from AWS Secrets Manager
     * @return The authentication type enum
     */
    public static SnowflakeAuthType determineAuthType(Map<String, String> credentials)
    {
        if (credentials == null || credentials.isEmpty()) {
            throw new IllegalArgumentException("Credentials cannot be null or empty");
        }

        SnowflakeAuthType authType;
        if (StringUtils.isNotBlank(credentials.get(SnowflakeConstants.PEM_PRIVATE_KEY))) {
            LOGGER.debug("Key-pair authentication detected");
            authType = SnowflakeAuthType.SNOWFLAKE_JWT;
        }
        else if (StringUtils.isNotBlank(credentials.get(SnowflakeConstants.AUTH_CODE))) {
            LOGGER.debug("OAuth authentication detected");
            authType = SnowflakeAuthType.OAUTH;
        }
        else {
            LOGGER.debug("Password authentication detected");
            authType = SnowflakeAuthType.SNOWFLAKE;
        }
        return authType;
    }

    /**
     * Creates a PrivateKey object from the PEM-formatted private key string.
     * Supports both encrypted and unencrypted private keys using Java standard libraries.
     * 
     * @param privateKeyPem The PEM-formatted private key string
     * @param passphrase The passphrase for encrypted private keys (can be null for unencrypted keys)
     * @return PrivateKey object
     * @throws Exception if the private key cannot be parsed
     */
    public static PrivateKey createPrivateKey(String privateKeyPem, String passphrase) throws Exception
    {
        try {
            PrivateKeyInfo privateKeyInfo = null;
            Security.addProvider(new BouncyCastleProvider());
            // Read an object from the private key file.
            PEMParser pemParser = new PEMParser(new StringReader(privateKeyPem));
            Object pemObject = pemParser.readObject();
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Handle the case where the private key is encrypted.
                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            }
            else if (pemObject instanceof PrivateKeyInfo) {
                // Handle the case where the private key is unencrypted.
                privateKeyInfo = (PrivateKeyInfo) pemObject;
            }
            pemParser.close();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            return converter.getPrivateKey(privateKeyInfo);
        }
        catch (Exception e) {
            LOGGER.error("Failed to create private key from PEM string: ", e);
            throw new Exception("Invalid private key format: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the username from credentials, checking both "username" and "user" fields.
     * 
     * @param credentials The credentials map
     * @return The username value
     * @throws IllegalArgumentException if neither field is present or is empty
     */
    public static String getUsername(Map<String, String> credentials)
    {
        String username = credentials.get(SnowflakeConstants.SF_USER);
        if (StringUtils.isBlank(username)) {
            //for oauth and password auth type
            //this can be removed once changes to sfUser instead of username
            username = credentials.get(SnowflakeConstants.USERNAME);
        }
        if (StringUtils.isBlank(username)) {
            throw new IllegalArgumentException("Missing required parameter: username/sfUser");
        }
        return username;
    }

    /**
     * Validates that the credentials contain the required fields for the authentication type.
     * 
     * @param credentials The credentials map
     * @param authType The authentication type
     * @throws IllegalArgumentException if required fields are missing
     */
    public static void validateCredentials(Map<String, String> credentials, SnowflakeAuthType authType)
    {
        if (credentials == null || credentials.isEmpty()) {
            throw new IllegalArgumentException("Credentials cannot be null or empty");
        }
        // Check for sfUser (or "username" field)
        getUsername(credentials);

        switch (authType) {
            case SNOWFLAKE_JWT:
                if (StringUtils.isBlank(credentials.get(SnowflakeConstants.PEM_PRIVATE_KEY))) {
                    throw new IllegalArgumentException("pem_private_key is required for key-pair authentication");
                }
                // Note: Passphrase is optional - only required if the private key is encrypted
                break;
            case OAUTH:
                if (StringUtils.isBlank(credentials.get(SnowflakeConstants.AUTH_CODE))) {
                    throw new IllegalArgumentException("Auth code is required for OAuth authentication");
                }
                break;
            case SNOWFLAKE:
                if (StringUtils.isBlank(credentials.get(SnowflakeConstants.PASSWORD))) {
                    throw new IllegalArgumentException("password is required for password authentication");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }
}
