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
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * Snowflake OAuth credentials provider that manages OAuth token lifecycle.
 * This provider handles token refresh, expiration, and provides credential properties
 * for Snowflake OAuth connections.
 */
public class SnowflakeCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeCredentialsProvider.class);
    
    public static final String ACCESS_TOKEN = "access_token";
    public static final String FETCHED_AT = "fetched_at";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String EXPIRES_IN = "expires_in";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String USER = "user";

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
            credentialMap.get(USER),
            credentialMap.get(PASSWORD)
        );
    }

    @Override
    public Map<String, String> getCredentialMap()
    {
        try {
            String secretString = secretsManager.getSecret(oauthSecretName);
            Map<String, String> oauthConfig = objectMapper.readValue(secretString, Map.class);
            
            if (oauthConfig.containsKey(SnowflakeConstants.AUTH_CODE) && !oauthConfig.get(SnowflakeConstants.AUTH_CODE).isEmpty()) {
                // OAuth flow
                String accessToken = fetchAccessTokenFromSecret(oauthConfig);
                
                Map<String, String> credentialMap = new HashMap<>();
                credentialMap.put(USER, oauthConfig.get(USERNAME));
                credentialMap.put(PASSWORD, accessToken);
                credentialMap.put("authenticator", "oauth");
                
                return credentialMap;
            }
            else {
                // Fallback to standard credentials
                return Map.of(
                        USER, oauthConfig.get(USERNAME),
                        PASSWORD, oauthConfig.get(PASSWORD)
                );
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error retrieving Snowflake credentials: " + ex.getMessage(), ex);
        }
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            return oauthConfig.get(ACCESS_TOKEN);
        }
        return null;
    }

    private void saveTokenToSecretsManager(JSONObject tokenJson, Map<String, String> oauthConfig)
    {
        // Update token related fields
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        oauthConfig.put(ACCESS_TOKEN, tokenJson.getString(ACCESS_TOKEN));
        oauthConfig.put(REFRESH_TOKEN, tokenJson.getString(REFRESH_TOKEN));
        oauthConfig.put(EXPIRES_IN, String.valueOf(tokenJson.getInt(EXPIRES_IN)));
        oauthConfig.put(FETCHED_AT, String.valueOf(tokenJson.getLong(FETCHED_AT)));

        // Save updated secret
        secretsManager.getSecretsManager().putSecretValue(builder -> builder
                .secretId(this.oauthSecretName)
                .secretString(String.valueOf(new JSONObject(oauthConfig)))
                .build());
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
            JSONObject tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            accessToken = tokenJson.getString(ACCESS_TOKEN);
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
                JSONObject refreshed = refreshAccessToken(oauthConfig.get(REFRESH_TOKEN), tokenEndpoint, clientId, clientSecret);
                refreshed.put(REFRESH_TOKEN, oauthConfig.get(REFRESH_TOKEN));
                saveTokenToSecretsManager(refreshed, oauthConfig);
                accessToken = refreshed.getString(ACCESS_TOKEN);
            }
        }
        return accessToken;
    }

    private JSONObject getTokenFromAuthCode(String authCode, String redirectUri, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=authorization_code"
                + "&code=" + authCode
                + "&redirect_uri=" + redirectUri;

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private JSONObject refreshAccessToken(String refreshToken, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=refresh_token"
                + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private JSONObject requestToken(String requestBody, String tokenEndpoint, String clientId, String clientSecret) throws Exception
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
            throw new RuntimeException("Failed: " + responseCode + " - " + response);
        }

        JSONObject tokenJson = new JSONObject(response);
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
