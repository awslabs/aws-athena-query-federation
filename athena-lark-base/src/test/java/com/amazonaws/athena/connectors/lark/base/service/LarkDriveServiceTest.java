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

import com.amazonaws.athena.connectors.lark.base.model.LarkDatabaseRecord;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllFolderResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LarkDriveServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

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
        public CloseableHttpResponse execute(HttpGet request) throws IOException {
            return createMockResponse(responseBody, statusCode, reasonPhrase);
        }

        @Override
        public CloseableHttpResponse execute(HttpPost request) throws IOException {
            // For the tenant access token
            String successBody = "{\"code\":0,\"msg\":\"success\",\"tenant_access_token\":\"test_token\",\"expire\":7200}";
            return createMockResponse(successBody, 200, "OK");
        }

        public CloseableHttpResponse createMockResponse(String body, int code, String phrase) throws IOException {
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

    @Test
    public void testGetLarkBasesSinglePage() throws Exception {
        ListAllFolderResponse.DriveFile file1 = ListAllFolderResponse.DriveFile.builder().name("table1").token("token1").type("bitable").build();
        ListAllFolderResponse.DriveFile file2 = ListAllFolderResponse.DriveFile.builder().name("table2").token("token2").type("bitable").build();

        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(List.of(file1, file2))
                .nextPageToken("")
                .hasMore(false)
                .build();

        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder()
                .code(0)
                .msg("success")
                .data(listData)
                .build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertEquals(2, result.size());
        assertEquals("table1", result.get(0).name());
        assertEquals("token1", result.get(0).id());
        assertEquals("table2", result.get(1).name());
        assertEquals("token2", result.get(1).id());
    }

    @Test
    public void testGetLarkBasesMultiplePages() throws Exception {
        ListAllFolderResponse.DriveFile file1 = ListAllFolderResponse.DriveFile.builder().name("table1").token("token1").type("bitable").build();
        ListAllFolderResponse.ListData listData1 = ListAllFolderResponse.ListData.builder()
                .files(List.of(file1))
                .nextPageToken("page2")
                .hasMore(true)
                .build();
        ListAllFolderResponse page1Response = ListAllFolderResponse.builder().code(0).msg("success").data(listData1).build();

        ListAllFolderResponse.DriveFile file2 = ListAllFolderResponse.DriveFile.builder().name("table2").token("token2").type("bitable").build();
        ListAllFolderResponse.ListData listData2 = ListAllFolderResponse.ListData.builder()
                .files(List.of(file2))
                .nextPageToken("")
                .hasMore(false)
                .build();
        ListAllFolderResponse page2Response = ListAllFolderResponse.builder().code(0).msg("success").data(listData2).build();

        String page1Body = objectMapper.writeValueAsString(page1Response);
        String page2Body = objectMapper.writeValueAsString(page2Response);

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(page1Body, 200, "OK") {
            private boolean firstCall = true;

            @Override
            public CloseableHttpResponse execute(HttpGet request) throws IOException {
                if (firstCall) {
                    firstCall = false;
                    return createMockResponse(page1Body, 200, "OK");
                } else {
                    return createMockResponse(page2Body, 200, "OK");
                }
            }
        };

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertEquals(2, result.size());
        assertEquals("table1", result.get(0).name());
        assertEquals("token1", result.get(0).id());
        assertEquals("table2", result.get(1).name());
        assertEquals("token2", result.get(1).id());
    }

    @Test
    public void testGetLarkBasesEmptyResult() throws Exception {
        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(List.of())
                .nextPageToken("")
                .hasMore(false)
                .build();
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder().code(0).msg("success").data(listData).build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetLarkBasesMixedFileTypes() throws Exception {
        ListAllFolderResponse.DriveFile file1 = ListAllFolderResponse.DriveFile.builder().name("table1").token("token1").type("bitable").build();
        ListAllFolderResponse.DriveFile file2 = ListAllFolderResponse.DriveFile.builder().name("document").token("token2").type("doc").build();

        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(List.of(file1, file2))
                .nextPageToken("")
                .hasMore(false)
                .build();
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder().code(0).msg("success").data(listData).build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertEquals(1, result.size());
        assertEquals("table1", result.get(0).name());
        assertEquals("token1", result.get(0).id());
    }

    @Test
    public void testGetLarkBasesApiError() throws Exception {
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder().code(1).msg("error").build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        assertThrows(RuntimeException.class, () -> larkDriveService.getLarkBases("folderToken"));
    }

    @Test
    public void testGetLarkBasesIOException() {
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper("", 200, "OK") {
            @Override
            public CloseableHttpResponse execute(HttpGet request) throws IOException {
                throw new IOException("test exception");
            }
        };

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        assertThrows(RuntimeException.class, () -> larkDriveService.getLarkBases("folderToken"));
    }

    @Test
    public void testGetLarkBasesTokenRefreshException() {
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(null, 0, null) {
            @Override
            public CloseableHttpResponse execute(HttpPost request) throws IOException {
                throw new IOException("Failed to refresh token");
            }
        };

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        assertThrows(RuntimeException.class, () -> larkDriveService.getLarkBases("folderToken"));
    }

    @Test
    public void testGetLarkBasesNoMoreData() throws Exception {
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder().code(1254002).msg("no more data").build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testConstructorWithoutHttpClient() {
        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret");
        assertNotNull(larkDriveService);
    }

    @Test
    public void testGetLarkBasesWithNullPageToken() throws Exception {
        ListAllFolderResponse.DriveFile file1 = ListAllFolderResponse.DriveFile.builder().name("table1").token("token1").type("bitable").build();
        ListAllFolderResponse.ListData listData1 = ListAllFolderResponse.ListData.builder()
                .files(List.of(file1))
                .nextPageToken("page2")
                .hasMore(true)
                .build();
        ListAllFolderResponse page1Response = ListAllFolderResponse.builder().code(0).msg("success").data(listData1).build();

        ListAllFolderResponse.DriveFile file2 = ListAllFolderResponse.DriveFile.builder().name("table2").token("token2").type("bitable").build();
        ListAllFolderResponse.ListData listData2 = ListAllFolderResponse.ListData.builder()
                .files(List.of(file2))
                .nextPageToken(null)
                .hasMore(true)
                .build();
        ListAllFolderResponse page2Response = ListAllFolderResponse.builder().code(0).msg("success").data(listData2).build();

        String page1Body = objectMapper.writeValueAsString(page1Response);
        String page2Body = objectMapper.writeValueAsString(page2Response);

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(page1Body, 200, "OK") {
            private boolean firstCall = true;

            @Override
            public CloseableHttpResponse execute(HttpGet request) throws IOException {
                if (firstCall) {
                    firstCall = false;
                    return createMockResponse(page1Body, 200, "OK");
                } else {
                    return createMockResponse(page2Body, 200, "OK");
                }
            }
        };

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertEquals(2, result.size());
        assertEquals("table1", result.get(0).name());
        assertEquals("token1", result.get(0).id());
        assertEquals("table2", result.get(1).name());
        assertEquals("token2", result.get(1).id());
    }

    @Test
    public void testGetLarkBasesWithNullFiles() throws Exception {
        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(null)
                .nextPageToken("")
                .hasMore(false)
                .build();
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder().code(0).msg("success").data(listData).build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        List<LarkDatabaseRecord> result = larkDriveService.getLarkBases("folderToken");

        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetLarkBasesNonZeroNon1254002Code() throws Exception {
        ListAllFolderResponse apiResponse = ListAllFolderResponse.builder()
                .code(500)
                .msg("Internal Server Error")
                .build();

        String responseBody = objectMapper.writeValueAsString(apiResponse);
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper(responseBody, 200, "OK");

        LarkDriveService larkDriveService = new LarkDriveService("appId", "appSecret", mockHttpClient);
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                larkDriveService.getLarkBases("folderToken")
        );

        assertTrue(exception.getMessage().contains("Failed to get records for folder: folderToken"));
        assertTrue(exception.getCause() instanceof java.io.IOException);
        assertTrue(exception.getCause().getMessage().contains("Failed to retrieve tables for folder"));
        assertTrue(exception.getCause().getMessage().contains("Internal Server Error"));
    }
}
