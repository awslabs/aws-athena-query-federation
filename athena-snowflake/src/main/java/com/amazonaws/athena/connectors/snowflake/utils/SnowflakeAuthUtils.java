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

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE_PASSWORD;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE_PRIVATE_KEY;

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
     * Gets the appropriate credential provider based on the authentication type.
     *
     * @param secretsManager SecretsManager client
     * @param secretName Secret name
     * @param configOptions Configuration options
     * @return CredentialsProvider
     */
    public static CredentialsProvider getCredentialProvider(
            SecretsManagerClient secretsManager,
            String secretName,
            Map<String, String> configOptions)
    {
        String authType = configOptions.getOrDefault(AUTH_TYPE, AUTH_TYPE_PASSWORD);

        if (AUTH_TYPE_PRIVATE_KEY.equals(authType)) {
            LOGGER.debug("Using private key authentication for Snowflake");
            return new SnowflakePrivateKeyCredentialProvider(secretsManager, secretName);
        }

        // Default to the standard username/password credential provider
        LOGGER.debug("Using password authentication for Snowflake");
        return new DefaultCredentialsProvider(getSecret(secretsManager, secretName));
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
     * Extracts the secret name from a connection string.
     *
     * @param connectionString Connection string
     * @return Secret name, or null if not found
     */
    public static String extractSecretNameFromConnectionString(String connectionString)
    {
        if (connectionString == null) {
            return null;
        }

        // Look for ${secretName} format secret reference
        if (connectionString.contains("${") && connectionString.contains("}")) {
            return connectionString.replaceAll(".*\\$\\{([^}]*)\\}.*", "$1");
        }
        // Look for secret_name=secretName format
        else if (connectionString.contains("secret_name=")) {
            return extractParameterFromConnectionString(connectionString, "secret_name");
        }
        // Look for secret=secretName format (for backward compatibility)
        else if (connectionString.contains("secret=")) {
            return connectionString.replaceAll(".*secret=([^&;]*).*", "$1");
        }

        return null;
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
        String[] possibleParams = {"secret_name", "auth_type", "role", "warehouse", "schema"};
        for (String param : possibleParams) {
            String value = extractParameterFromConnectionString(connectionString, param);
            if (value != null) {
                extractedParams.put(param, value);
                LOGGER.debug("Extracted {} = {} from connection string", param,
                    param.toLowerCase().contains("secret") ? "****" : value);
            }
        }
        
        // Extract ${secretName} format
        if (connectionString.contains("${") && connectionString.contains("}")) {
            String placeholder = connectionString.replaceAll(".*\\$\\{([^}]*)\\}.*", "$1");
            if (placeholder != null && !placeholder.isEmpty()) {
                extractedParams.put("secret_name", placeholder);
                LOGGER.debug("Extracted secret_name = {} from placeholder ${{{}}}", placeholder, placeholder);
            }
        }
        
        return extractedParams;
    }

    /**
     * Updates configuration options from a connection string.
     * Does nothing if the map is unmodifiable.
     *
     * @param connectionString Connection string
     * @param configOptions Configuration options
     * @return Whether the update was successful
     */
    public static boolean updateConfigOptionsFromConnectionString(String connectionString, Map<String, String> configOptions)
    {
        if (connectionString == null || configOptions == null) {
            return false;
        }
        
        // Check if the map is modifiable
        boolean isModifiable = true;
        try {
            // Try to set a temporary value to test
            String testKey = "__test_key__";
            configOptions.put(testKey, "test");
            configOptions.remove(testKey);
        }
        catch (UnsupportedOperationException e) {
            // Map is unmodifiable
            isModifiable = false;
            LOGGER.warn("Config options map is unmodifiable, parameters from connection string will not be added");
            return false;
        }
        
        // If modifiable, extract parameters and add them
        Map<String, String> extractedParams = extractParametersFromConnectionString(connectionString);
        configOptions.putAll(extractedParams);
        return true;
    }

    /**
     * Gets the secret name from environment variables, configuration options, or parent class.
     *
     * @param configOptions Configuration options
     * @param parentCredentialProvider Parent credential provider (optional)
     * @return Secret name, or null if not found
     */
    public static String getSecretNameFromSources(Map<String, String> configOptions, CredentialsProvider parentCredentialProvider)
    {
        // 1. Get connection string from environment variable and extract secret name
        String secretName = null;
        if (System.getenv("JDBC_CONNECTION_STRING") != null) {
            secretName = extractSecretNameFromConnectionString(System.getenv("JDBC_CONNECTION_STRING"));
        }

        // 2. Get from configOptions
        if ((secretName == null || secretName.isEmpty()) && configOptions.containsKey("secret_name")) {
            secretName = configOptions.get("secret_name");
        }

        // 3. Get from parent credential provider
        if ((secretName == null || secretName.isEmpty()) && parentCredentialProvider != null) {
            // If available from parent class, use the parent credential provider directly
            return null;
        }

        return secretName;
    }

    /**
     * Updates configuration options from the secret.
     *
     * @param secretsManager SecretsManager client
     * @param secretName Secret name
     * @param configOptions Configuration options
     */
    public static void updateConfigOptionsFromSecret(
            SecretsManagerClient secretsManager,
            String secretName,
            Map<String, String> configOptions)
    {
        try {
            String secretString = getSecret(secretsManager, secretName);
            Map<String, String> secretMap = new ObjectMapper().readValue(secretString, Map.class);

            // Get auth_type from secret and add to configuration options
            String authType = secretMap.get("auth_type");
            if (authType != null) {
                LOGGER.debug("Setting auth_type from secret: {}", authType);
                configOptions.put(AUTH_TYPE, authType);
            }
        }
        catch (Exception e) {
            LOGGER.error("Error updating config options from secret", e);
        }
    }
}
