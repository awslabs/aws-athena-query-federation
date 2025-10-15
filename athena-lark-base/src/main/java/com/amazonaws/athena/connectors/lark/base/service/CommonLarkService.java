/*-
 * #%L
 * glue-lark-base-crawler
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
package com.amazonaws.athena.connectors.lark.base.service;

import com.amazonaws.athena.connectors.lark.base.model.request.TenantAccessTokenRequest;
import com.amazonaws.athena.connectors.lark.base.model.response.TenantAccessTokenResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class CommonLarkService
{
    protected static final String LARK_API_BASE_URL = "https://open.larksuite.com/open-apis";
    protected static final String LARK_AUTH_URL = LARK_API_BASE_URL + "/auth";

    // HTTP header constants
    protected static final String HEADER_AUTHORIZATION = "Authorization";
    protected static final String HEADER_CONTENT_TYPE = "Content-Type";
    protected static final String CONTENT_TYPE_JSON = "application/json";
    protected static final String AUTH_BEARER_PREFIX = "Bearer ";

    protected String tenantAccessToken;
    protected long tokenExpiry;

    private final String larkAppId;
    private final String larkAppSecret;
    protected HttpClientWrapper httpClient;
    protected ObjectMapper objectMapper = new ObjectMapper();

    public CommonLarkService(String larkAppId, String larkAppSecret)
    {
        this.larkAppId = larkAppId;
        this.larkAppSecret = larkAppSecret;
        this.httpClient = new HttpClientWrapper();
    }

    public CommonLarkService(String larkAppId, String larkAppSecret, HttpClientWrapper httpClient)
    {
        this.larkAppId = larkAppId;
        this.larkAppSecret = larkAppSecret;
        this.httpClient = httpClient;
    }

    /**
     * Refresh tenant access token. We use synchronized to ensure that only one thread can refresh the token at a time.
     *
     * @throws IOException If failed to refresh tenant access token
     * @see "https://open.larksuite.com/document/server-docs/getting-started/api-access-token/auth-v3/tenant_access_token_internal"
     */
    protected synchronized void refreshTenantAccessToken() throws IOException
    {
        boolean needsRefresh = tenantAccessToken == null || System.currentTimeMillis() >= tokenExpiry;

        if (!needsRefresh) {
            return;
        }

        HttpPost request = new HttpPost(LARK_AUTH_URL + "/v3/tenant_access_token/internal");
        request.setHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON);

        TenantAccessTokenRequest tokenRequest = new TenantAccessTokenRequest(larkAppId, larkAppSecret);
        String requestBody = objectMapper.writeValueAsString(tokenRequest);
        request.setEntity(new StringEntity(requestBody));

        if (httpClient == null) {
            throw new IllegalStateException("HTTP client not yet initialized");
        }

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            String responseBody = EntityUtils.toString(response.getEntity());
            TenantAccessTokenResponse tokenResponse = objectMapper.readValue(responseBody, TenantAccessTokenResponse.class);

            if (tokenResponse.code() == 0 && tokenResponse.tenantAccessToken() != null
                    && !tokenResponse.tenantAccessToken().isEmpty()) {
                tenantAccessToken = tokenResponse.tenantAccessToken();
                tokenExpiry = System.currentTimeMillis() + (tokenResponse.expire() * 1000L);
                return;
            }

            tenantAccessToken = null;
            tokenExpiry = 0;

            throw new IOException("Failed to obtain Lark access token: " + tokenResponse.msg());
        }
    }
}
