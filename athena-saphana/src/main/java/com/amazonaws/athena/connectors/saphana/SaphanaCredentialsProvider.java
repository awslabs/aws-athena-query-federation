/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SAP HANA OAuth credentials provider that manages OAuth token lifecycle.
 * This provider handles token refresh, expiration, and provides credential properties
 * for SAP HANA OAuth connections.
 * SAP HANA OAuth Authentication:
 * - Supports client_credentials grant type for OAuth 2.0
 * - When using OAuth, username should be empty and password should be the access token
 * - Falls back to traditional username/password authentication when OAuth is not configured
 */
public class SaphanaCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SaphanaCredentialsProvider.class);
    
    private static final String ACCESS_TOKEN = "access_token";
    private static final String FETCHED_AT = "fetched_at";
    private static final String EXPIRES_IN = "expires_in";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String USER = "user";

    private final String oauthSecretName;
    private final CachableSecretsManager secretsManager;
    private final ObjectMapper objectMapper;

    public SaphanaCredentialsProvider(String oauthSecretName)
    {
        this(oauthSecretName, SecretsManagerClient.create());
    }

    @VisibleForTesting
    public SaphanaCredentialsProvider(String oauthSecretName, SecretsManagerClient secretsClient)
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
            
            if (isOAuthConfigured(oauthConfig)) {
                // OAuth flow - SAP HANA uses client_credentials grant
                String accessToken = fetchAccessTokenFromSecret(oauthConfig);
                
                Map<String, String> credentialMap = new HashMap<>();
                // For SAP HANA OAuth: username is empty, password is the access token
                credentialMap.put(USER, "");
                credentialMap.put(PASSWORD, accessToken);

                return credentialMap;
            }
            else {
                // Fallback to standard credentials
                return Map.of(
                        USER, oauthConfig.getOrDefault(USERNAME, ""),
                        PASSWORD, oauthConfig.getOrDefault(PASSWORD, "")
                );
            }
        }
        catch (Exception ex) {
            throw new AthenaConnectorException(
                "Error retrieving SAP HANA credentials: " + ex.getMessage(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }
    }

    private boolean isOAuthConfigured(Map<String, String> oauthConfig)
    {
        return oauthConfig.containsKey(SaphanaConstants.CLIENT_ID) && 
               !oauthConfig.get(SaphanaConstants.CLIENT_ID).isEmpty() &&
               oauthConfig.containsKey(SaphanaConstants.CLIENT_SECRET) && 
               !oauthConfig.get(SaphanaConstants.CLIENT_SECRET).isEmpty() &&
               oauthConfig.containsKey(SaphanaConstants.TOKEN_URL) && 
               !oauthConfig.get(SaphanaConstants.TOKEN_URL).isEmpty();
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            return oauthConfig.get(ACCESS_TOKEN);
        }
        return null;
    }

    private String fetchAndStoreNewToken(Map<String, String> oauthConfig) throws Exception
    {
        String clientId = oauthConfig.get(SaphanaConstants.CLIENT_ID);
        String clientSecret = oauthConfig.get(SaphanaConstants.CLIENT_SECRET);
        String tokenEndpoint = oauthConfig.get(SaphanaConstants.TOKEN_URL);

        HttpURLConnection conn = getHttpURLConnection(tokenEndpoint, clientId, clientSecret);
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = "grant_type=client_credentials".getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }

        int responseCode = conn.getResponseCode();
        InputStream responseStream = (responseCode >= 200 && responseCode < 300) ?
                conn.getInputStream() : conn.getErrorStream();

        String responseBody = new BufferedReader(new InputStreamReader(responseStream))
                .lines()
                .collect(Collectors.joining("\n"));

        if (responseCode != 200) {
            throw new AthenaConnectorException(
                "Failed to obtain access token: " + responseCode + " - " + responseBody,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }

        JsonNode tokenResponse;
        try {
            tokenResponse = objectMapper.readTree(responseBody);
        }
        catch (Exception e) {
            throw new AthenaConnectorException(
                "Failed to parse token response JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }
        String accessToken = tokenResponse.get(ACCESS_TOKEN).asText();
        long expiresIn = tokenResponse.get(EXPIRES_IN).asLong();
        long fetchedAt = System.currentTimeMillis() / 1000;

        oauthConfig.put(ACCESS_TOKEN, accessToken);
        oauthConfig.put(EXPIRES_IN, String.valueOf(expiresIn));
        oauthConfig.put(FETCHED_AT, String.valueOf(fetchedAt));

        ObjectNode updatedSecretJson = objectMapper.createObjectNode();
        for (Map.Entry<String, String> entry : oauthConfig.entrySet()) {
            updatedSecretJson.put(entry.getKey(), entry.getValue());
        }

        String secretString;
        try {
            secretString = objectMapper.writeValueAsString(updatedSecretJson);
        }
        catch (Exception e) {
            throw new AthenaConnectorException(
                "Failed to serialize updated secret JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }

        secretsManager.getSecretsManager().putSecretValue(builder -> builder
                .secretId(this.oauthSecretName)
                .secretString(secretString)
                .build());

        return accessToken;
    }

    // Replace fetchAccessTokenFromSecret to use fetchAndStoreNewToken
    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken = loadTokenFromSecretsManager(oauthConfig);
        if (accessToken == null) {
            LOGGER.debug("First time auth. Using client_credentials grant...");
            return fetchAndStoreNewToken(oauthConfig);
        }
        else {
            long expiresIn = Long.parseLong(oauthConfig.getOrDefault(EXPIRES_IN, "0"));
            long fetchedAt = Long.parseLong(oauthConfig.getOrDefault(FETCHED_AT, "0"));
            long now = System.currentTimeMillis() / 1000;
            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid.");
                return accessToken;
            }
            else {
                LOGGER.debug("Access token expired. Getting new token...");
                return fetchAndStoreNewToken(oauthConfig);
            }
        }
    }

    @VisibleForTesting
    static HttpURLConnection getHttpURLConnection(String tokenEndpoint, String clientId, String clientSecret) throws IOException
    {
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        String auth = clientId + ":" + clientSecret;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        return conn;
    }
}
