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

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.OAuthAccessTokenCredentials;
import com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider;
import com.google.common.annotations.VisibleForTesting;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * OAuth credentials provider for Saphana.
 */
public class SaphanaOAuthCredentialsProvider extends OAuthCredentialsProvider
{
    protected static final String TOKEN_URL = "token_url";

    public SaphanaOAuthCredentialsProvider()
    {
        super();
    }

    @VisibleForTesting
    protected SaphanaOAuthCredentialsProvider(HttpClient httpClient)
    {
        super(httpClient);
    }

    @Override
    protected boolean isOAuthConfigured(Map<String, String> oauthConfig)
    {
        return oauthConfig.containsKey(CredentialsConstants.CLIENT_ID) &&
                !oauthConfig.get(CredentialsConstants.CLIENT_ID).isEmpty() &&
                oauthConfig.containsKey(CredentialsConstants.CLIENT_SECRET) &&
                !oauthConfig.get(CredentialsConstants.CLIENT_SECRET).isEmpty() &&
                oauthConfig.containsKey(TOKEN_URL) &&
                !oauthConfig.get(TOKEN_URL).isEmpty();
    }

    @Override
    protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
    {
        String clientId = secretMap.get(CredentialsConstants.CLIENT_ID);
        String clientSecret = secretMap.get(CredentialsConstants.CLIENT_SECRET);
        String tokenEndpoint = secretMap.get(TOKEN_URL);

        String auth = clientId + ":" + clientSecret;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        return HttpRequest.newBuilder()
                .uri(URI.create(tokenEndpoint))
                .header("Authorization", "Basic " + encodedAuth)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                .build();
    }

    @Override
    public Credentials getCredential()
    {
        // Parent always returns OAuthAccessTokenCredentials (or throws exception)
        OAuthAccessTokenCredentials parentCredentials = (OAuthAccessTokenCredentials) super.getCredential();
        
        // Convert to Saphana-specific credentials that use "password" property
        return new SaphanaOAuthAccessTokenCredentials(parentCredentials.getAccessToken());
    }
}
