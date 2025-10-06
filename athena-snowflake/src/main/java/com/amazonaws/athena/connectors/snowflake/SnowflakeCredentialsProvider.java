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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.USER;
import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType.OAUTH;
import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils.getUsername;
import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils.validateCredentials;

/**
 * Snowflake credentials provider that manages multiple authentication methods.
 * This provider handles OAuth token lifecycle, key-pair authentication, and password authentication.
 * Authentication method is automatically determined based on the secret contents.
 */
public class SnowflakeCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeCredentialsProvider.class);
    
    public static final String ACCESS_TOKEN = "access_token";
    public static final String FETCHED_AT = "fetched_at";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String EXPIRES_IN = "expires_in";
    private final String oauthSecretName;
    private final CachableSecretsManager secretsManager;
    private final ObjectMapper objectMapper;

    public SnowflakeCredentialsProvider(String oauthSecretName)
    {
        this(oauthSecretName, SecretsManagerClient.create());
    }

    @VisibleForTesting
    public SnowflakeCredentialsProvider(String oauthSecretName, SecretsManagerClient secretsClient)
    {
        this.oauthSecretName = Validate.notNull(oauthSecretName, "oauthSecretName must not be null");
        this.secretsManager = new CachableSecretsManager(secretsClient);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public DefaultCredentials getCredential()
    {
        Map<String, String> credentialMap = getCredentialMap();
        return new DefaultCredentials(
            credentialMap.get(SnowflakeConstants.USER),
            credentialMap.get(SnowflakeConstants.PASSWORD)
        );
    }

    @Override
    public Map<String, String> getCredentialMap()
    {
        try {
            String secretString = secretsManager.getSecret(oauthSecretName);
            Map<String, String> secretMap = objectMapper.readValue(secretString, Map.class);
            
            // Determine authentication type based on secret contents
            SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(secretMap);
            // Validate credentials once after determining auth type
            validateCredentials(secretMap, authType);
            switch (authType) {
                case SNOWFLAKE_JWT:
                    // Key-pair authentication
                    return handleKeyPairAuthentication(secretMap);
                case OAUTH:
                    // OAuth flow
                    return handleOAuthAuthentication(secretMap);
                case SNOWFLAKE:
                default:
                    // Password authentication (backward compatible)
                    return handlePasswordAuthentication(secretMap);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error retrieving Snowflake credentials: " + ex.getMessage(), ex);
        }
    }

    /**
     * Handles key-pair authentication.
     */
    private Map<String, String> handleKeyPairAuthentication(Map<String, String> oauthConfig)
    {
        Map<String, String> credentialMap = new HashMap<>();
        String username = getUsername(oauthConfig);
        credentialMap.put(USER, username);
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY, oauthConfig.get(SnowflakeConstants.PEM_PRIVATE_KEY));
        credentialMap.put(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE, oauthConfig.get(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE));
        LOGGER.debug("Using key-pair authentication for user: {}", username);
        return credentialMap;
    }

    /**
     * Handles OAuth authentication.
     */
    private Map<String, String> handleOAuthAuthentication(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken = fetchAccessTokenFromSecret(oauthConfig);
        Map<String, String> credentialMap = new HashMap<>();
        String username = getUsername(oauthConfig);
        credentialMap.put(SnowflakeConstants.USER, username);
        credentialMap.put(SnowflakeConstants.PASSWORD, accessToken);
        credentialMap.put(SnowflakeConstants.AUTHENTICATOR, OAUTH.getValue());
        LOGGER.debug("Using OAuth authentication for user: {}", username);
        return credentialMap;
    }

    /**
     * Handles password authentication (backward compatible).
     */
    private Map<String, String> handlePasswordAuthentication(Map<String, String> oauthConfig)
    {
        Map<String, String> credentialMap = new HashMap<>();
        credentialMap.put(SnowflakeConstants.USER, getUsername(oauthConfig));
        credentialMap.put(SnowflakeConstants.PASSWORD, oauthConfig.get(SnowflakeConstants.PASSWORD));
        LOGGER.debug("Using password authentication for user: {}", getUsername(oauthConfig));
        return credentialMap;
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            return oauthConfig.get(ACCESS_TOKEN);
        }
        return null;
    }

    private void saveTokenToSecretsManager(ObjectNode tokenJson, Map<String, String> oauthConfig)
    {
        // Update token related fields
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        oauthConfig.put(ACCESS_TOKEN, tokenJson.get(ACCESS_TOKEN).asText());
        oauthConfig.put(REFRESH_TOKEN, tokenJson.get(REFRESH_TOKEN).asText());
        oauthConfig.put(EXPIRES_IN, String.valueOf(tokenJson.get(EXPIRES_IN).asInt()));
        oauthConfig.put(FETCHED_AT, String.valueOf(tokenJson.get(FETCHED_AT).asLong()));

        // Save updated secret
        try {
            String updatedSecretString = objectMapper.writeValueAsString(oauthConfig);
            secretsManager.getSecretsManager().putSecretValue(builder -> builder
                    .secretId(this.oauthSecretName)
                    .secretString(updatedSecretString)
                    .build());
        }
        catch (Exception e) {
            LOGGER.error("Failed to save updated secret: ", e);
            throw new RuntimeException("Failed to save updated secret: ", e);
        }
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken;
        String clientId = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_ID), "Missing required property: client_id");
        String tokenEndpoint = Validate.notNull(oauthConfig.get(SnowflakeConstants.TOKEN_URL), "Missing required property: token_url");
        String redirectUri = Validate.notNull(oauthConfig.get(SnowflakeConstants.REDIRECT_URI), "Missing required property: redirect_uri");
        String clientSecret = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_SECRET), "Missing required property: client_secret");
        String authCode = Validate.notNull(oauthConfig.get(SnowflakeConstants.AUTH_CODE), "Missing required property: auth_code");

        accessToken = loadTokenFromSecretsManager(oauthConfig);

        if (accessToken == null) {
            LOGGER.debug("First time auth. Using authorization_code...");
            ObjectNode tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            accessToken = tokenJson.get(ACCESS_TOKEN).asText();
        }
        else {
            long expiresIn = Long.parseLong(oauthConfig.get(EXPIRES_IN));
            long fetchedAt = Long.parseLong(oauthConfig.getOrDefault(FETCHED_AT, String.valueOf(0L)));
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid.");
            }
            else {
                LOGGER.debug("Access token expired. Using refresh_token...");
                ObjectNode refreshed = refreshAccessToken(oauthConfig.get(REFRESH_TOKEN), tokenEndpoint, clientId, clientSecret);
                refreshed.put(REFRESH_TOKEN, oauthConfig.get(REFRESH_TOKEN));
                saveTokenToSecretsManager(refreshed, oauthConfig);
                accessToken = refreshed.get(ACCESS_TOKEN).asText();
            }
        }
        return accessToken;
    }

    private ObjectNode getTokenFromAuthCode(String authCode, String redirectUri, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=authorization_code"
                + "&code=" + authCode
                + "&redirect_uri=" + redirectUri;

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private ObjectNode refreshAccessToken(String refreshToken, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=refresh_token"
                + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private ObjectNode requestToken(String requestBody, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        HttpURLConnection conn = getHttpURLConnection(tokenEndpoint, clientId, clientSecret);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) ?
                conn.getInputStream() : conn.getErrorStream();

        String response = new BufferedReader(new InputStreamReader(is))
                .lines()
                .reduce("", (acc, line) -> acc + line);

        if (responseCode != 200) {
            LOGGER.error("OAuth token request failed with status: {} - {}", responseCode, response);
            throw new AthenaConnectorException("OAuth authentication failed with status: " + responseCode, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString()).build());
        }

        ObjectNode tokenJson = objectMapper.readValue(response, ObjectNode.class);
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        return tokenJson;
    }

    static HttpURLConnection getHttpURLConnection(String tokenEndpoint, String clientId, String clientSecret) throws IOException
    {
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        String authHeader = Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + authHeader);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        return conn;
    }
}
