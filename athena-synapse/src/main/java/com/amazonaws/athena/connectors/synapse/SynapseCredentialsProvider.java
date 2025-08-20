/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class SynapseCredentialsProvider implements CredentialsProvider
{
    // Constants for OAuth token response fields
    private static final String ACCESS_TOKEN = "access_token";
    private static final String FETCHED_AT = "fetched_at";
    private static final String EXPIRES_IN = "expires_in";

    // Constants for OAuth configuration fields
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String TENANT_ID = "tenant_id";

    // Constants for basic authentication fields
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String USERNAME = "username";

    private static final String GRANT_TYPE = "client_credentials";
    private static final String SCOPE = "https://sql.azuresynapse.net/.default";
    private static final String TOKEN_ENDPOINT_TEMPLATE = "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
    private static final long TOKEN_REFRESH_BUFFER_SECONDS = 300;

    private final String secretName;
    private final CachableSecretsManager secretsManager;
    private final ObjectMapper objectMapper;

    public SynapseCredentialsProvider(String secretName)
    {
        this.secretName = secretName;
        this.secretsManager = new CachableSecretsManager(SecretsManagerClient.create());
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
            String secretString = secretsManager.getSecret(secretName);
            Map<String, String> oauthConfig = objectMapper.readValue(secretString, Map.class);

            Map<String, String> credentialMap = new HashMap<>();

            if (!isOAuthConfigured(oauthConfig)) {
                credentialMap.put(USER, oauthConfig.get(USERNAME));
                credentialMap.put(PASSWORD, oauthConfig.get(PASSWORD));
            }

            return credentialMap;
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse Synapse credentials JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
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
                "Failed to retrieve Synapse credentials from Secrets Manager",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }
    }

    public String getOAuthAccessToken()
    {
        try {
            String secretValue = secretsManager.getSecret(secretName);
            Map<String, String> oauthConfig = objectMapper.readValue(secretValue, Map.class);

            if (isOAuthConfigured(oauthConfig)) {
                return fetchAccessToken(oauthConfig);
            }
            return null;
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse OAuth configuration JSON",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
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
                "Failed to retrieve OAuth configuration from Secrets Manager",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
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

    private boolean isOAuthConfigured(Map<String, String> oauthConfig)
    {
        return oauthConfig.containsKey(CLIENT_ID) &&
                !oauthConfig.get(CLIENT_ID).isEmpty() &&
                oauthConfig.containsKey(CLIENT_SECRET) &&
                !oauthConfig.get(CLIENT_SECRET).isEmpty() &&
                oauthConfig.containsKey(TENANT_ID) &&
                !oauthConfig.get(TENANT_ID).isEmpty();
    }

    private String fetchAccessToken(Map<String, String> oauthConfig) throws IOException, InterruptedException
    {
        String accessToken = oauthConfig.get(ACCESS_TOKEN);

        if (accessToken != null &&
                oauthConfig.containsKey(FETCHED_AT) &&
                oauthConfig.containsKey(EXPIRES_IN)) {
            long fetchedAt = Long.parseLong(oauthConfig.get(FETCHED_AT));
            long expiresIn = Long.parseLong(oauthConfig.get(EXPIRES_IN));
            long now = Instant.now().getEpochSecond();

            if (now < (fetchedAt + expiresIn - TOKEN_REFRESH_BUFFER_SECONDS)) {
                return accessToken;
            }
        }

        return fetchAndStoreNewToken(oauthConfig);
    }

    private String fetchAndStoreNewToken(Map<String, String> oauthConfig) throws IOException, InterruptedException
    {
        String clientId = oauthConfig.get(CLIENT_ID);
        String clientSecret = oauthConfig.get(CLIENT_SECRET);
        String tenantId = oauthConfig.get(TENANT_ID);

        String tokenUrl = String.format(TOKEN_ENDPOINT_TEMPLATE, tenantId);
        String requestBody = String.format(
                "grant_type=%s&scope=%s&client_id=%s&client_secret=%s",
                GRANT_TYPE, SCOPE, clientId, clientSecret
        );

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            String errorMessage = String.format("Failed to fetch access token. Status code: %d", response.statusCode());
            if (response.statusCode() == 401 || response.statusCode() == 403) {
                throw new AthenaConnectorException(
                    errorMessage,
                    ErrorDetails.builder()
                        .errorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString())
                        .build()
                );
            }
            if (response.statusCode() == 429) {
                throw new AthenaConnectorException(
                    errorMessage,
                    ErrorDetails.builder()
                        .errorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION.toString())
                        .build()
                );
            }
            throw new AthenaConnectorException(
                errorMessage,
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                    .build()
            );
        }

        try {
            JsonNode tokenResponse = objectMapper.readTree(response.body());
            String accessToken = tokenResponse.get(ACCESS_TOKEN).asText();
            long expiresIn = tokenResponse.get(EXPIRES_IN).asLong();
            long fetchedAt = Instant.now().getEpochSecond();

            oauthConfig.put(ACCESS_TOKEN, accessToken);
            oauthConfig.put(EXPIRES_IN, String.valueOf(expiresIn));
            oauthConfig.put(FETCHED_AT, String.valueOf(fetchedAt));

            ObjectNode updatedSecretJson = objectMapper.createObjectNode();
            for (Map.Entry<String, String> entry : oauthConfig.entrySet()) {
                updatedSecretJson.put(entry.getKey(), entry.getValue());
            }

            try {
                secretsManager.getSecretsManager().putSecretValue(PutSecretValueRequest.builder()
                        .secretId(secretName)
                        .secretString(updatedSecretJson.toString())
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
                    "Failed to update access token in Secrets Manager",
                    ErrorDetails.builder()
                        .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                        .build()
                );
            }

            return accessToken;
        }
        catch (JsonProcessingException e) {
            throw new AthenaConnectorException(
                "Failed to parse OAuth token response",
                ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString())
                    .build()
            );
        }
    }
}
