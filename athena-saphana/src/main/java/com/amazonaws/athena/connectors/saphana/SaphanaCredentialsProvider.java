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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.EncryptionFailureException;
import software.amazon.awssdk.services.secretsmanager.model.InternalServiceErrorException;
import software.amazon.awssdk.services.secretsmanager.model.InvalidParameterException;
import software.amazon.awssdk.services.secretsmanager.model.InvalidRequestException;
import software.amazon.awssdk.services.secretsmanager.model.LimitExceededException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import software.amazon.awssdk.utils.Validate;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

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
    
    // Constants for OAuth configuration fields
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String TOKEN_URL = "token_url";

    // Constants for OAuth token response fields
    private static final String ACCESS_TOKEN = "access_token";
    private static final String FETCHED_AT = "fetched_at";
    private static final String EXPIRES_IN = "expires_in";

    // Constants for basic authentication fields
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String USER = "user";

    private final String oauthSecretName;
    private final CachableSecretsManager secretsManager;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public SaphanaCredentialsProvider(String oauthSecretName)
    {
        this(oauthSecretName, SecretsManagerClient.create(), HttpClient.newHttpClient());
    }

    @VisibleForTesting
    public SaphanaCredentialsProvider(String oauthSecretName, SecretsManagerClient secretsClient, HttpClient httpClient)
    {
        this.oauthSecretName = Validate.notNull(oauthSecretName, "oauthSecretName must not be null");
        this.secretsManager = new CachableSecretsManager(secretsClient);
        this.objectMapper = new ObjectMapper();
        this.httpClient = httpClient;
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
        return oauthConfig.containsKey(CLIENT_ID) && 
               !oauthConfig.get(CLIENT_ID).isEmpty() &&
               oauthConfig.containsKey(CLIENT_SECRET) && 
               !oauthConfig.get(CLIENT_SECRET).isEmpty() &&
               oauthConfig.containsKey(TOKEN_URL) && 
               !oauthConfig.get(TOKEN_URL).isEmpty();
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            return oauthConfig.get(ACCESS_TOKEN);
        }
        return null;
    }

    private String fetchAndStoreNewToken(Map<String, String> oauthConfig) throws IOException, InterruptedException
    {
        String clientId = oauthConfig.get(CLIENT_ID);
        String clientSecret = oauthConfig.get(CLIENT_SECRET);
        String tokenEndpoint = oauthConfig.get(TOKEN_URL);

        String auth = clientId + ":" + clientSecret;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Authorization", "Basic " + encodedAuth)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int responseCode = response.statusCode();
        String responseBody = response.body();

        if (responseCode != 200) {
            throw new AthenaConnectorException(
                "Failed to obtain access token",
                "HTTP " + responseCode + " - " + responseBody,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        JsonNode tokenResponse;
        try {
            tokenResponse = objectMapper.readTree(responseBody);
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse OAuth token response",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
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
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse OAuth token response",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        try {
            secretsManager.getSecretsManager().putSecretValue(builder -> builder
                    .secretId(this.oauthSecretName)
                    .secretString(secretString)
                    .build());
        }
        catch (ResourceNotFoundException e) {
            throw new AthenaConnectorException(
                "Secret not found: " + oauthSecretName,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString())
                    .build()
            );
        }
        catch (InvalidParameterException | InvalidRequestException e) {
            throw new AthenaConnectorException(
                "Invalid request to Secrets Manager",
                e.getMessage(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString())
                    .build()
            );
        }
        catch (LimitExceededException e) {
            throw new AthenaConnectorException(
                "Rate limit exceeded for Secrets Manager",
                e.getMessage(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString())
                    .build()
            );
        }
        catch (EncryptionFailureException | InternalServiceErrorException e) {
            throw new AthenaConnectorException(
                "AWS Secrets Manager internal error",
                e.getMessage(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }
        catch (SecretsManagerException e) {
            throw new AthenaConnectorException(
                "Failed to update OAuth token in Secrets Manager",
                e.getMessage(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }

        return accessToken;
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws IOException, InterruptedException
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
}
