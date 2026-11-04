/*-
 * #%L
 * athena-lark-base
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

import com.amazonaws.athena.connectors.lark.base.model.response.TenantAccessTokenResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommonLarkServiceTest {

    private static final String TEST_APP_ID = "testAppId";
    private static final String TEST_APP_SECRET = "testAppSecret";

    private static class MockHttpClientWrapper extends HttpClientWrapper {
        private String responseBody;
        private int statusCode;
        private String reasonPhrase;

        public MockHttpClientWrapper(String responseBody, int statusCode, String reasonPhrase) {
            this.responseBody = responseBody;
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }

        @Override
        public CloseableHttpResponse execute(HttpPost request) throws IOException {
            return createMockResponse(responseBody, statusCode, reasonPhrase);
        }

        private CloseableHttpResponse createMockResponse(String body, int code, String phrase) throws IOException {
            CloseableHttpResponse response = mock(CloseableHttpResponse.class);
            StatusLine statusLine = mock(StatusLine.class);
            HttpEntity entity = new StringEntity(body);

            when(statusLine.getStatusCode()).thenReturn(code);
            when(statusLine.getReasonPhrase()).thenReturn(phrase);
            when(response.getStatusLine()).thenReturn(statusLine);
            when(response.getEntity()).thenReturn(entity);

            return response;
        }
    }

    private static class TestableCommonLarkService extends CommonLarkService {
        public TestableCommonLarkService(String larkAppId, String larkAppSecret, HttpClientWrapper httpClient) {
            super(larkAppId, larkAppSecret, httpClient);
        }

        @Override
        public void refreshTenantAccessToken() throws IOException {
            super.refreshTenantAccessToken();
        }
        public String getTenantAccessToken() {
            return tenantAccessToken;
        }
    }

    @Test
    public void refreshTenantAccessToken_success() throws Exception {
        String successBody = "{\"code\":0,\"msg\":\"success\",\"tenant_access_token\":\"test_token\",\"expire\":7200}";
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(successBody, 200, "OK");
        TestableCommonLarkService service = new TestableCommonLarkService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        service.refreshTenantAccessToken();

        assertEquals("test_token", service.getTenantAccessToken());
    }

    @Test
    public void refreshTenantAccessToken_alreadyFresh() throws Exception {
        String successBody = "{\"code\":0,\"msg\":\"success\",\"tenant_access_token\":\"test_token\",\"expire\":7200}";
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(successBody, 200, "OK");
        TestableCommonLarkService service = new TestableCommonLarkService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        service.refreshTenantAccessToken();
        assertEquals("test_token", service.getTenantAccessToken());

        // Should not refresh again
        service.refreshTenantAccessToken();
        assertEquals("test_token", service.getTenantAccessToken());
    }

    @Test
    public void refreshTenantAccessToken_apiError() {
        String errorBody = "{\"code\":1,\"msg\":\"error\"}";
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(errorBody, 200, "OK");
        TestableCommonLarkService service = new TestableCommonLarkService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(IOException.class, service::refreshTenantAccessToken);
    }

    @Test
    public void refreshTenantAccessToken_nullResponse() {
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(null, 200, "OK");
        TestableCommonLarkService service = new TestableCommonLarkService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(Exception.class, service::refreshTenantAccessToken);
    }

    @Test
    public void refreshTenantAccessToken_emptyToken() {
        String emptyTokenBody = "{\"code\":0,\"msg\":\"success\",\"tenant_access_token\":\"\",\"expire\":7200}";
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(emptyTokenBody, 200, "OK");
        TestableCommonLarkService service = new TestableCommonLarkService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(IOException.class, service::refreshTenantAccessToken);
    }
}
