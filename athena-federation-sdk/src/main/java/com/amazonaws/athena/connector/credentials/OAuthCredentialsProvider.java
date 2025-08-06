/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * Base class for OAuth credential providers that handle token lifecycle management.
 * Supports both OAuth token and username/password authentication.
 */
public abstract class OAuthCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuthCredentialsProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Constants for OAuth token response fields
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
    public static final String FETCHED_AT = "fetched_at";

    // Constants for OAuth configuration fields
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SECRET = "client_secret";

    // Constants for basic authentication fields
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String USER = "user";

    private final String secretName;
    private final CachableSecretsManager secretsManager;
    private final HttpClient httpClient;

    protected OAuthCredentialsProvider(String secretName)
    {
        this(secretName, SecretsManagerClient.create(), HttpClient.newBuilder().build());
    }

    protected OAuthCredentialsProvider(String secretName, SecretsManagerClient secretsClient, HttpClient httpClient)
    {
        this.secretName = secretName;
        this.secretsManager = new CachableSecretsManager(secretsClient);
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
            String secretString = secretsManager.getSecret(secretName);
            Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);

            if (isOAuthConfigured(secretMap)) {
                String accessToken = getOAuthAccessToken(secretMap);
                return mapOAuthCredentials(accessToken);
            }
            else {
                // Fallback to username/password
                return Map.of(
                    USER, secretMap.getOrDefault(USERNAME, ""),
                    PASSWORD, secretMap.getOrDefault(PASSWORD, "")
                );
            }
        }
        catch (ResourceNotFoundException e) {
            throw new AthenaConnectorException(
                "Secret not found: " + secretName,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString())
                    .build()
            );
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse secret JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }
        catch (IOException e) {
            throw new AthenaConnectorException(
                "Failed to communicate with OAuth endpoint",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString())
                    .build()
            );
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AthenaConnectorException(
                "OAuth token fetch interrupted",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString())
                    .build()
            );
        }
    }

    /**
     * Maps OAuth access token to JDBC connection properties.
     * Each provider can specify how the token should be used (e.g. as password, as property).
     */
    protected abstract Map<String, String> mapOAuthCredentials(String accessToken);

    /**
     * Checks if OAuth is configured by verifying required fields exist.
     */
    protected abstract boolean isOAuthConfigured(Map<String, String> secretMap);

    /**
     * Builds the token request for the specific OAuth provider.
     */
    protected abstract HttpRequest buildTokenRequest(Map<String, String> secretMap);

    private String getOAuthAccessToken(Map<String, String> secretMap) throws IOException, InterruptedException
    {
        String accessToken = secretMap.get(ACCESS_TOKEN);
        if (accessToken != null) {
            long expiresIn = Long.parseLong(secretMap.getOrDefault(EXPIRES_IN, "0"));
            long fetchedAt = Long.parseLong(secretMap.getOrDefault(FETCHED_AT, "0"));
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid");
                return accessToken;
            }
            LOGGER.debug("Access token expired, fetching new token");
        }

        return fetchAndStoreNewToken(secretMap);
    }

    private String fetchAndStoreNewToken(Map<String, String> secretMap) throws IOException, InterruptedException
    {
        HttpRequest request = buildTokenRequest(secretMap);
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 401 || response.statusCode() == 403) {
            throw new AthenaConnectorException(
                "Failed to fetch access token: Invalid credentials",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString())
                    .build()
            );
        }
        if (response.statusCode() == 429) {
            throw new AthenaConnectorException(
                "Failed to fetch access token: Rate limit exceeded",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString())
                    .build()
            );
        }
        if (response.statusCode() != 200) {
            throw new AthenaConnectorException(
                "Failed to fetch access token: HTTP " + response.statusCode(),
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        JsonNode tokenResponse = OBJECT_MAPPER.readTree(response.body());
        JsonNode accessTokenNode = tokenResponse.get(ACCESS_TOKEN);
        JsonNode expiresInNode = tokenResponse.get(EXPIRES_IN);
        
        if (accessTokenNode == null || expiresInNode == null) {
            throw new AthenaConnectorException(
                "Invalid OAuth response: Missing required fields",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        String accessToken = accessTokenNode.asText();
        long expiresIn;
        try {
            expiresIn = expiresInNode.asLong();
        }
        catch (NumberFormatException e) {
            throw new AthenaConnectorException(
                "Invalid OAuth response: Invalid expires_in value",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        long fetchedAt = System.currentTimeMillis() / 1000;

        secretMap.put(ACCESS_TOKEN, accessToken);
        secretMap.put(EXPIRES_IN, String.valueOf(expiresIn));
        secretMap.put(FETCHED_AT, String.valueOf(fetchedAt));

        String secretString;
        try {
            secretString = OBJECT_MAPPER.writeValueAsString(secretMap);
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to serialize OAuth credentials",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }

        try {
            secretsManager.getSecretsManager().putSecretValue(builder -> builder
                .secretId(secretName)
                .secretString(secretString)
                .build());
        }
        catch (ResourceNotFoundException e) {
            throw new AthenaConnectorException(
                "Secret not found: " + secretName,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString())
                    .build()
            );
        }
        catch (SecretsManagerException e) {
            throw new AthenaConnectorException(
                "Failed to update OAuth credentials in Secrets Manager",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }

        return accessToken;
    }
}
