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
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.ACCESS_TOKEN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.EXPIRES_IN;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.FETCHED_AT;

/**
 * Base class for OAuth credential providers.
 * Handles OAuth token lifecycle management.
 */
public abstract class OAuthCredentialsProvider implements InitializableCredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OAuthCredentialsProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String secretName;
    private CachableSecretsManager secretsManager;
    private Map<String, String> secretMap;
    private HttpClient httpClient;

    protected OAuthCredentialsProvider()
    {
        // Initialized later via initialize() method
    }

    @VisibleForTesting
    protected OAuthCredentialsProvider(HttpClient httpClient)
    {
        this.httpClient = httpClient;
    }

    @Override
    public void initialize(String secretName, Map<String, String> secretMap, CachableSecretsManager secretsManager)
    {
        this.secretName = secretName;
        this.secretMap = secretMap;
        this.secretsManager = secretsManager;
        this.httpClient = (this.httpClient != null)
                ? this.httpClient
                : HttpClient.newBuilder().build();
    }

    @Override
    public Credentials getCredential()
    {
        try {
            String accessToken = getOAuthAccessToken(secretMap);
            return new OAuthAccessTokenCredentials(accessToken);
        }
        catch (ResourceNotFoundException e) {
            throw new AthenaConnectorException("Secret not found: " + secretName,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException("Failed to parse secret JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
        catch (IOException e) {
            throw new AthenaConnectorException("Failed to communicate with OAuth endpoint",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AthenaConnectorException("OAuth token fetch interrupted",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
    }

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
        String cached = secretMap.get(ACCESS_TOKEN);
        if (cached != null) {
            long expiresIn = Long.parseLong(secretMap.getOrDefault(EXPIRES_IN, "0"));
            long fetchedAt = Long.parseLong(secretMap.getOrDefault(FETCHED_AT, "0"));
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid");
                return cached;
            }
            LOGGER.debug("Access token expired, fetching new token");
        }

        HttpResponse<String> response = httpClient.send(
            buildTokenRequest(secretMap), HttpResponse.BodyHandlers.ofString());

        int sc = response.statusCode();
        if (sc == 401 || sc == 403) {
            throw new AthenaConnectorException("Failed to fetch access token: Invalid credentials",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString())
                    .errorMessage("HTTP Status: " + sc)
                    .build());
        }
        if (sc == 429) {
            throw new AthenaConnectorException("Failed to fetch access token: Rate limit exceeded",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString())
                    .errorMessage("HTTP Status: " + sc)
                    .build());
        }
        if (sc != 200) {
            throw new AthenaConnectorException("Failed to fetch access token: HTTP " + sc,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .errorMessage("Unexpected HTTP status: " + sc)
                    .build());
        }

        JsonNode tokenResponse;
        try {
            tokenResponse = OBJECT_MAPPER.readTree(response.body());
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException("Failed to parse OAuth token response",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }

        JsonNode accessTokenNode = tokenResponse.get(ACCESS_TOKEN);
        JsonNode expiresInNode = tokenResponse.get(EXPIRES_IN);

        if (accessTokenNode == null || expiresInNode == null) {
            throw new AthenaConnectorException("Invalid OAuth response: Missing required fields",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .errorMessage("Response missing access_token or expires_in fields")
                    .build());
        }

        String accessToken = accessTokenNode.asText();
        long expiresIn = expiresInNode.asLong();
        long fetchedAt = System.currentTimeMillis() / 1000;

        secretMap.put(ACCESS_TOKEN, accessToken);
        secretMap.put(EXPIRES_IN, String.valueOf(expiresIn));
        secretMap.put(FETCHED_AT, String.valueOf(fetchedAt));

        try {
            String secretString = OBJECT_MAPPER.writeValueAsString(secretMap);
            secretsManager.getSecretsManager().putSecretValue(b -> b
                .secretId(secretName)
                .secretString(secretString)
                .build());
        }
        catch (ResourceNotFoundException e) {
            throw new AthenaConnectorException("Secret not found: " + secretName,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException("Failed to serialize OAuth credentials",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }
        catch (SecretsManagerException e) {
            throw new AthenaConnectorException("Failed to update OAuth credentials in Secrets Manager",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .errorMessage(e.getMessage())
                    .build());
        }

        return accessToken;
    }
}
