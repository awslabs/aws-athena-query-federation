/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;

/**
 * Azure Data Lake Gen2 OAuth credentials provider that manages OAuth token lifecycle.
 * This provider handles token refresh, expiration, and provides credential properties
 * for Data Lake Gen2 OAuth connections.
 * Azure Data Lake Gen2 OAuth Authentication:
 * - Supports client_credentials grant type for OAuth 2.0
 * - Uses accessToken property for JDBC connection
 * - Falls back to traditional username/password authentication when OAuth is not configured
 */
public class DataLakeGen2CredentialsProvider extends OAuthCredentialsProvider
{
    private static final String TENANT_ID = "tenant_id";
    private static final String TOKEN_ENDPOINT_FORMAT = "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
    private static final String SCOPE = "https://sql.azuresynapse.net/.default";

    public DataLakeGen2CredentialsProvider(String secretName)
    {
        super(secretName);
    }

    @VisibleForTesting
    public DataLakeGen2CredentialsProvider(String secretName, SecretsManagerClient secretsClient, HttpClient httpClient)
    {
        super(secretName, secretsClient, httpClient);
    }

    @Override
    protected Map<String, String> mapOAuthCredentials(String accessToken)
    {
        Map<String, String> props = new HashMap<>();
        props.put(DataLakeGen2Constants.ACCESS_TOKEN_PROPERTY, accessToken);
        return props;
    }

    @Override
    protected boolean isOAuthConfigured(Map<String, String> secretMap)
    {
        return secretMap.containsKey(CLIENT_ID) && 
               !secretMap.get(CLIENT_ID).isEmpty() &&
               secretMap.containsKey(CLIENT_SECRET) && 
               !secretMap.get(CLIENT_SECRET).isEmpty() &&
               secretMap.containsKey(TENANT_ID) && 
               !secretMap.get(TENANT_ID).isEmpty();
    }

    @Override
    protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
    {
        String clientId = secretMap.get(CLIENT_ID);
        String clientSecret = secretMap.get(CLIENT_SECRET);
        String tenantId = secretMap.get(TENANT_ID);
        String tokenEndpoint = String.format(TOKEN_ENDPOINT_FORMAT, tenantId);

        String formData = String.format(
                "grant_type=client_credentials&scope=%s&client_id=%s&client_secret=%s",
                SCOPE, clientId, clientSecret);

        return HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(formData))
                .build();
    }
}
