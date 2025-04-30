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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Snowflake authentication.
 */
public final class SnowflakeAuthUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeAuthUtils.class);

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private SnowflakeAuthUtils()
    {
        // Utility class should not be instantiated
    }

    /**
     * Gets a SecretsManager client.
     *
     * @return SecretsManager client
     */
    public static SecretsManagerClient getSecretsManager()
    {
        return SecretsManagerClient.builder().build();
    }

    /**
     * Gets the secret value from SecretsManager.
     *
     * @param secretsManager SecretsManager client
     * @param secretName Secret name
     * @return Secret value
     */
    public static String getSecret(SecretsManagerClient secretsManager, String secretName)
    {
        GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();
        GetSecretValueResponse getSecretValueResponse = secretsManager.getSecretValue(getSecretValueRequest);
        return getSecretValueResponse.secretString();
    }
    
    /**
     * Extracts a parameter from a connection string.
     *
     * @param connectionString Connection string
     * @param paramName Parameter name
     * @return Parameter value, or null if not found
     */
    public static String extractParameterFromConnectionString(String connectionString, String paramName)
    {
        if (connectionString == null || paramName == null) {
            return null;
        }
        
        String regex = paramName + "=([^&]*)";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
        java.util.regex.Matcher matcher = pattern.matcher(connectionString);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
    
    /**
     * Extracts parameters from a connection string and returns them as a new map.
     * Does not modify the original map.
     *
     * @param connectionString Connection string
     * @return A new map containing the extracted parameters
     */
    public static Map<String, String> extractParametersFromConnectionString(String connectionString)
    {
        Map<String, String> extractedParams = new HashMap<>();
        
        if (connectionString == null) {
            return extractedParams;
        }
        
        LOGGER.debug("Extracting parameters from connection string");
        
        // Extract common parameters
        String[] possibleParams = {"secret", "role", "warehouse", "schema"};
        for (String param : possibleParams) {
            String value = extractParameterFromConnectionString(connectionString, param);
            if (value != null) {
                extractedParams.put(param, value);
                LOGGER.debug("Extracted {} = {} from connection string", param, value);
            }
        }
        
        // Extract ${secretName} format
        if (connectionString.contains("${") && connectionString.contains("}")) {
            String placeholder = connectionString.replaceAll(".*\\$\\{([^}]*)\\}.*", "$1");
            if (placeholder != null && !placeholder.isEmpty()) {
                extractedParams.put("secret", placeholder);
                LOGGER.debug("Extracted secret = {} from placeholder ${{{}}}", placeholder, placeholder);
            }
        }
        
        return extractedParams;
    }

    /**
     * Gets the secret name from various sources in order of priority:
     * 1. Connection string (secret parameter)
     * 2. Connection string (${secretName} format)
     * 3. Environment variable (secret)
     *
     * @param connectionString JDBC connection string
     * @return Secret name, or null if not found
     */
    public static String getSecretName(String connectionString)
    {
        Logger logger = LoggerFactory.getLogger(SnowflakeAuthUtils.class);
        String secretName = null;

        // Extract secret name from connection string
        if (connectionString != null) {
            // Extract secret parameter
            secretName = extractParameterFromConnectionString(connectionString, "secret");

            // Extract ${secretName} format (if secret parameter doesn't exist)
            if (secretName == null && connectionString.contains("${") && connectionString.contains("}")) {
                secretName = connectionString.replaceAll(".*\\$\\{([^}]*)\\}.*", "$1");
                logger.debug("Extracted secret from placeholder: {}", secretName);
            }
        }

        // Get secret name from environment variable
        if (secretName == null) {
            secretName = System.getenv("secret");
            if (secretName != null) {
                logger.debug("Using secret from environment variable: {}", secretName);
            }
        }

        return secretName;
    }

    /**
     * Gets a credential provider using the default connection string.
     * This method encapsulates the entire credential provider creation process:
     * 1. Gets the connection string from the "default" environment variable
     * 2. Extracts the secret name from the connection string
     * 3. Creates and returns the appropriate credential provider based on the secret content
     *
     * @return CredentialsProvider based on the connection string and secret content
     */
    public static CredentialsProvider getCredentialProviderWithDefault()
    {
        Logger logger = LoggerFactory.getLogger(SnowflakeAuthUtils.class);
        logger.debug("getCredentialProviderWithDefault called");

        // Get connection string from default environment variable
        String connectionString = System.getenv("default");

        // Get secret name
        String secretName = getSecretName(connectionString);

        // Create and return credential provider based on secret content
        return createCredentialProvider(secretName);
    }

    /**
     * Creates a credential provider based on the secret content.
     * If the secret contains a privateKey, uses private key authentication.
     * If the secret contains a password, uses password authentication.
     *
     * @param secretName Secret name
     * @return CredentialsProvider
     */
    public static CredentialsProvider createCredentialProvider(String secretName)
    {
        Logger logger = LoggerFactory.getLogger(SnowflakeAuthUtils.class);

        if (secretName == null || secretName.isEmpty()) {
            logger.error("No secret name found for authentication");
            throw new RuntimeException("No secret name found for authentication");
        }

        // Create SecretsManager client
        SecretsManagerClient secretsManager = getSecretsManager();

        try {
            // Get secret content
            String secretString = getSecret(secretsManager, secretName);

            // Use TypeReference to provide proper generic type information
            Map<String, String> secretMap = new ObjectMapper().readValue(secretString,
                    new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});

            // Check if secret contains privateKey
            if (secretMap.containsKey("privateKey") && secretMap.get("privateKey") != null) {
                logger.debug("Secret contains privateKey, using private key authentication");
                return new SnowflakePrivateKeyCredentialProvider(secretsManager, secretName);
            }
            // Check if secret contains password
            else if (secretMap.containsKey("password") && secretMap.get("password") != null) {
                logger.debug("Secret contains password, using password authentication");
                return new DefaultCredentialsProvider(secretString);
            }
            // If neither privateKey nor password is found, throw an exception
            else {
                logger.error("Secret does not contain privateKey or password");
                throw new RuntimeException("Secret must contain either privateKey or password");
            }
        }
        catch (Exception e) {
            logger.error("Error creating credential provider", e);
            throw new RuntimeException("Failed to create credential provider: " + e.getMessage(), e);
        }
    }
}
