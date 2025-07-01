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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
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
        if (credentials.containsKey(SnowflakeConstants.PRIVATE_KEY) && 
            credentials.get(SnowflakeConstants.PRIVATE_KEY) != null && 
            !credentials.get(SnowflakeConstants.PRIVATE_KEY).trim().isEmpty()) {
            LOGGER.debug("Key-pair authentication detected");
            authType = SnowflakeAuthType.SNOWFLAKE_JWT;
        }
        else if (credentials.containsKey(SnowflakeConstants.AUTH_CODE) && 
                 credentials.get(SnowflakeConstants.AUTH_CODE) != null && 
                 !credentials.get(SnowflakeConstants.AUTH_CODE).trim().isEmpty()) {
            LOGGER.debug("OAuth authentication detected");
            authType = SnowflakeAuthType.OAUTH;
        }
        else {
            LOGGER.debug("Password authentication detected");
            authType = SnowflakeAuthType.SNOWFLAKE;
        }

        // Validate credentials once after determining auth type
        validateCredentials(credentials, authType);
        
        return authType;
    }

    /**
     * Creates a PrivateKey object from the PEM-formatted private key string.
     * 
     * @param privateKeyPem The PEM-formatted private key string
     * @return PrivateKey object
     * @throws Exception if the private key cannot be parsed
     */
    public static PrivateKey createPrivateKey(String privateKeyPem) throws Exception
    {
        try {
            // Remove PEM headers and footers
            String privateKeyContent = privateKeyPem
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                    .replace("-----END RSA PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");

            // Decode the base64 encoded key
            byte[] keyBytes = Base64.getDecoder().decode(privateKeyContent);

            // Create PKCS8 key spec
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);

            // Get key factory and generate private key
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(keySpec);
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
        String username = credentials.get(SnowflakeConstants.USERNAME);
        if (username == null || username.trim().isEmpty()) {
            username = credentials.get(SnowflakeConstants.USER);
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username is required");
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

        switch (authType) {
            case SNOWFLAKE_JWT:
                // Check for username (either "username" or "user" field)
                getUsername(credentials);
                if (!credentials.containsKey(SnowflakeConstants.PRIVATE_KEY) || 
                    credentials.get(SnowflakeConstants.PRIVATE_KEY) == null || 
                    credentials.get(SnowflakeConstants.PRIVATE_KEY).trim().isEmpty()) {
                    throw new IllegalArgumentException("Private key is required for key-pair authentication");
                }
                break;
            case OAUTH:
                // Check for username (either "username" or "user" field)
                getUsername(credentials);
                if (!credentials.containsKey(SnowflakeConstants.AUTH_CODE) || 
                    credentials.get(SnowflakeConstants.AUTH_CODE) == null || 
                    credentials.get(SnowflakeConstants.AUTH_CODE).trim().isEmpty()) {
                    throw new IllegalArgumentException("Auth code is required for OAuth authentication");
                }
                break;
            case SNOWFLAKE:
                // Check for username (either "username" or "user" field)
                getUsername(credentials);
                if (!credentials.containsKey(SnowflakeConstants.PASSWORD) ||
                    credentials.get(SnowflakeConstants.PASSWORD) == null ||
                    credentials.get(SnowflakeConstants.PASSWORD).trim().isEmpty()) {
                    throw new IllegalArgumentException("Password is required for password authentication");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + authType);
        }
    }

    /**
     * Gets the default credential provider for Snowflake.
     * This method provides backward compatibility by returning the main SnowflakeCredentialsProvider.
     * 
     * @return The default credential provider
     */
    public static com.amazonaws.athena.connector.credentials.CredentialsProvider getCredentialProviderWithDefault()
    {
        // This method is used for backward compatibility
        // The actual credential provider should be injected by the calling code
        LOGGER.debug("getCredentialProviderWithDefault called - this should be overridden by the actual implementation");
        return null;
    }
}
