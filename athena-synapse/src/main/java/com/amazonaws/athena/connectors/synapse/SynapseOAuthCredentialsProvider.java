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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.Map;

/**
 * OAuth credentials provider for Azure Synapse.
 */
public class SynapseOAuthCredentialsProvider extends OAuthCredentialsProvider
{
    private static final String TOKEN_ENDPOINT_FORMAT = "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
    private static final String SCOPE = "https://sql.azuresynapse.net/.default";
    private static final String TENANT_ID = "tenant_id";

    public SynapseOAuthCredentialsProvider()
    {
        super();
    }

    @VisibleForTesting
    protected SynapseOAuthCredentialsProvider(HttpClient httpClient)
    {
        super(httpClient);
    }

    @Override
    protected boolean isOAuthConfigured(Map<String, String> secretMap)
    {
        return secretMap.containsKey(CredentialsConstants.CLIENT_ID) &&
               !secretMap.get(CredentialsConstants.CLIENT_ID).isEmpty() &&
               secretMap.containsKey(CredentialsConstants.CLIENT_SECRET) &&
               !secretMap.get(CredentialsConstants.CLIENT_SECRET).isEmpty() &&
               secretMap.containsKey(TENANT_ID) &&
               !secretMap.get(TENANT_ID).isEmpty();
    }

    @Override
    protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
    {
        String clientId = secretMap.get(CredentialsConstants.CLIENT_ID);
        String clientSecret = secretMap.get(CredentialsConstants.CLIENT_SECRET);
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
