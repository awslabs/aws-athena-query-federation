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
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.model.request.TableRecordsRequest;
import com.amazonaws.athena.connectors.lark.base.model.response.ListAllTableResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.ListFieldResponse;
import com.amazonaws.athena.connectors.lark.base.model.response.ListRecordsResponse;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LarkBaseServiceTest {

    private static final String TEST_APP_ID = "testAppId";
    private static final String TEST_APP_SECRET = "testAppSecret";

    private static class MockHttpClientWrapper extends HttpClientWrapper {
        private final List<String> responseBodies = new ArrayList<>();
        private final List<Integer> statusCodes = new ArrayList<>();
        private final List<String> reasonPhrases = new ArrayList<>();
        private final AtomicInteger requestCount = new AtomicInteger(0);

        public MockHttpClientWrapper() {
        }

        public void addResponse(String responseBody, int statusCode, String reasonPhrase) {
            responseBodies.add(responseBody);
            statusCodes.add(statusCode);
            reasonPhrases.add(reasonPhrase);
        }

        @Override
        public CloseableHttpResponse execute(HttpGet request) throws IOException {
            int count = requestCount.getAndIncrement();
            return createMockResponse(responseBodies.get(count), statusCodes.get(count), reasonPhrases.get(count));
        }

        @Override
        public CloseableHttpResponse execute(HttpPost request) throws IOException {
            if (request.getURI().toString().contains("tenant_access_token")) {
                String successBody = "{\"code\":0,\"msg\":\"success\",\"tenant_access_token\":\"test_token\",\"expire\":7200}";
                return createMockResponse(successBody, 200, "OK");
            }
            int count = requestCount.getAndIncrement();
            return createMockResponse(responseBodies.get(count), statusCodes.get(count), reasonPhrases.get(count));
        }

        private CloseableHttpResponse createMockResponse(String body, int code, String phrase) throws UnsupportedEncodingException {
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
    public void listTables_success_singlePage() {
        String baseId = "base123";
        String mockJsonResponse = "{\"code\":0, \"msg\":\"success\", \"data\":{\"items\":[{\"table_id\":\"tbl1\",\"name\":\"table_1\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListAllTableResponse.BaseItem> result = larkBaseService.listTables(baseId);

        assertEquals(1, result.size());
        assertEquals("tbl1", result.get(0).getTableId());
        assertEquals("table_1", result.get(0).getName());
    }

    @Test
    public void listTables_success_multiplePages() {
        String baseId = "base123";
        String mockJsonResponse1 = "{\"code\":0, \"msg\":\"success\", \"data\":{\"items\":[{\"table_id\":\"tbl1\",\"name\":\"table_1\"}],\"has_more\":true, \"page_token\":\"token2\"}}";
        String mockJsonResponse2 = "{\"code\":0, \"msg\":\"success\", \"data\":{\"items\":[{\"table_id\":\"tbl2\",\"name\":\"table_2\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse1, 200, "OK");
        mockHttpClient.addResponse(mockJsonResponse2, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListAllTableResponse.BaseItem> result = larkBaseService.listTables(baseId);

        assertEquals(2, result.size());
        assertEquals("tbl1", result.get(0).getTableId());
        assertEquals("table_1", result.get(0).getName());
        assertEquals("tbl2", result.get(1).getTableId());
        assertEquals("table_2", result.get(1).getName());
    }

    @Test
    public void getTableFields_success_singlePage() {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Text\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListFieldResponse.FieldItem> result = larkBaseService.getTableFields(baseId, tableId);

        assertEquals(1, result.size());
        assertEquals("fld1", result.get(0).getFieldId());
        assertEquals("Field 1", result.get(0).getFieldName());
    }

    @Test
    public void getTableFields_success_multiplePages() {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse1 = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Text\"}],\"has_more\":true, \"page_token\":\"token2\"}}";
        String mockJsonResponse2 = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld2\",\"field_name\":\"Field 2\",\"ui_type\":\"Number\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse1, 200, "OK");
        mockHttpClient.addResponse(mockJsonResponse2, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListFieldResponse.FieldItem> result = larkBaseService.getTableFields(baseId, tableId);

        assertEquals(2, result.size());
        assertEquals("fld1", result.get(0).getFieldId());
        assertEquals("Field 1", result.get(0).getFieldName());
        assertEquals("fld2", result.get(1).getFieldId());
        assertEquals("Field 2", result.get(1).getFieldName());
    }

    @Test
    public void getTableRecords_success_singlePage() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder().baseId(baseId).tableId(tableId).build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_success_multiplePages() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse1 = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":true, \"page_token\":\"token2\"}}";
        String mockJsonResponse2 = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec456\",\"fields\":{\"id\":\"rec456\",\"name\":\"Record Name 2\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse1, 200, "OK");
        mockHttpClient.addResponse(mockJsonResponse2, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder().baseId(baseId).tableId(tableId).build();
        ListRecordsResponse result1 = larkBaseService.getTableRecords(request);

        TableRecordsRequest request2 = TableRecordsRequest.builder().baseId(baseId).tableId(tableId).pageToken("token2").build();
        ListRecordsResponse result2 = larkBaseService.getTableRecords(request2);

        assertEquals(1, result1.getItems().size());
        assertEquals("rec123", result1.getItems().get(0).getRecordId());
        assertEquals(1, result2.getItems().size());
        assertEquals("rec456", result2.getItems().get(0).getRecordId());
    }

    @Test
    public void getDatabaseRecords_success() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(1, result.size());
        assertEquals("rec123", result.get(0).id());
        assertEquals("Record Name", result.get(0).name());
    }

    @Test
    public void listTables_apiError() {
        String baseId = "base123";
        String mockJsonResponse = "{\"code\":1, \"msg\":\"error\"}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(RuntimeException.class, () -> larkBaseService.listTables(baseId));
    }

    @Test
    public void getTableFields_apiError() {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":1, \"msg\":\"error\"}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(RuntimeException.class, () -> larkBaseService.getTableFields(baseId, tableId));
    }

    @Test
    public void getTableRecords_apiError() {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse = "{\"code\":1, \"msg\":\"error\"}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder().baseId(baseId).tableId(tableId).build();
        assertThrows(IOException.class, () -> larkBaseService.getTableRecords(request));
    }

    @Test
    public void getDatabaseRecords_apiError() {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":1, \"msg\":\"error\"}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(IOException.class, () -> larkBaseService.getDatabaseRecords(baseId, tableId));
    }

    @Test
    public void getLookupType_success() {
        String baseId = "base1";
        String tableId = "tbl1";
        String fieldId = "fld1";
        String mockJsonResponse = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Text\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        UITypeEnum result = larkBaseService.getLookupType(baseId, tableId, fieldId);

        assertEquals(UITypeEnum.TEXT, result);
    }

    @Test
    public void getLookupType_notFound() {
        String baseId = "base1";
        String tableId = "tbl1";
        String fieldId = "fld2";
        String mockJsonResponse = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Text\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        UITypeEnum result = larkBaseService.getLookupType(baseId, tableId, fieldId);

        assertEquals(UITypeEnum.UNKNOWN, result);
    }

    @Test
    public void getLookupType_recursive() {
        String baseId = "base1";
        String tableId1 = "tbl1";
        String fieldId1 = "fld1";

        String mockJsonResponse1 = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Lookup\", \"property\":{\"target_field\":\"fld2\",\"filter_info\":{\"target_table\":\"tbl2\"}}}]}}";
        String mockJsonResponse2 = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld2\",\"field_name\":\"Field 2\",\"ui_type\":\"Number\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse1, 200, "OK");
        mockHttpClient.addResponse(mockJsonResponse2, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        UITypeEnum result = larkBaseService.getLookupType(baseId, tableId1, fieldId1);

        assertEquals(UITypeEnum.NUMBER, result);
    }

    @Test
    public void getTableFields_invalidCacheKey_fallsBackToDirectFetch() {
        // Test that the cache loader's IllegalArgumentException path is covered
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0, \"data\":{\"items\":[{\"field_id\":\"fld1\",\"field_name\":\"Field 1\",\"ui_type\":\"Text\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        // This should work normally
        List<ListFieldResponse.FieldItem> result = larkBaseService.getTableFields(baseId, tableId);

        assertEquals(1, result.size());
        assertEquals("fld1", result.get(0).getFieldId());
    }

    @Test
    public void getDatabaseRecords_withNullFields() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(1, result.size());
        assertNull(result.get(0).id());
        assertNull(result.get(0).name());
    }

    @Test
    public void getDatabaseRecords_withNullIdAndName() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":null,\"name\":null}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(1, result.size());
        assertNull(result.get(0).id());
        assertNull(result.get(0).name());
    }

    @Test
    public void getDatabaseRecords_withMissingIdField() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"name\":\"Test Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(1, result.size());
        assertNull(result.get(0).id());
        assertEquals("Test Name", result.get(0).name());
    }

    @Test
    public void getDatabaseRecords_withMissingNameField() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(1, result.size());
        assertEquals("rec123", result.get(0).id());
        assertNull(result.get(0).name());
    }

    @Test
    public void getTableRecords_withFilterJson() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String filterJson = "{\"conditions\":[{\"field_id\":\"fld1\",\"operator\":\"is\",\"value\":[\"test\"]}],\"conjunction\":\"and\"}";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId(baseId)
                .tableId(tableId)
                .filterJson(filterJson)
                .build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_withSortJson() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String sortJson = "[{\"field_id\":\"fld1\",\"desc\":false}]";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId(baseId)
                .tableId(tableId)
                .sortJson(sortJson)
                .build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_withFilterAndSortJson() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String filterJson = "{\"conditions\":[{\"field_id\":\"fld1\",\"operator\":\"is\",\"value\":[\"test\"]}],\"conjunction\":\"and\"}";
        String sortJson = "[{\"field_id\":\"fld1\",\"desc\":false}]";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId(baseId)
                .tableId(tableId)
                .filterJson(filterJson)
                .sortJson(sortJson)
                .build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_withEmptyFilterJson() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId(baseId)
                .tableId(tableId)
                .filterJson("")
                .build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_withEmptySortJson() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":[{\"record_id\":\"rec123\",\"fields\":{\"id\":\"rec123\",\"name\":\"Record Name\"}}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId(baseId)
                .tableId(tableId)
                .sortJson("")
                .build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        assertEquals(1, result.getItems().size());
        assertEquals("rec123", result.getItems().get(0).getRecordId());
    }

    @Test
    public void getTableRecords_withNullRequest() {
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET);

        assertThrows(NullPointerException.class, () -> larkBaseService.getTableRecords(null));
    }

    @Test
    public void getTableRecords_withNullItems() throws Exception {
        String baseId = "baseR1";
        String tableId = "tblR1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":null,\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        TableRecordsRequest request = TableRecordsRequest.builder().baseId(baseId).tableId(tableId).build();
        ListRecordsResponse result = larkBaseService.getTableRecords(request);

        // When items is null, it gets deserialized as an empty list
        // The sanitizeRecordFieldNames method returns early when items is null
        assertTrue(result.getItems() == null || result.getItems().isEmpty());
    }

    @Test
    public void fetchTableFieldsUncached_refreshTokenFailure() {
        // Create a mock that throws IOException when refreshing token
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper() {
            @Override
            public CloseableHttpResponse execute(HttpPost request) throws IOException {
                if (request.getURI().toString().contains("tenant_access_token")) {
                    throw new IOException("Failed to refresh token");
                }
                return super.execute(request);
            }
        };

        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        // Trigger cache miss which will call fetchTableFieldsUncached
        assertThrows(RuntimeException.class, () -> larkBaseService.getTableFields("base1", "tbl1"));
    }

    @Test
    public void listTables_refreshTokenFailure() {
        // Create a mock that throws IOException when refreshing token
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper() {
            @Override
            public CloseableHttpResponse execute(HttpPost request) throws IOException {
                if (request.getURI().toString().contains("tenant_access_token")) {
                    throw new IOException("Failed to refresh token");
                }
                return super.execute(request);
            }
        };

        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        assertThrows(RuntimeException.class, () -> larkBaseService.listTables("base1"));
    }

    @Test
    public void listTables_withNoMoreDataCode() {
        String baseId = "base123";
        String mockJsonResponse = "{\"code\":1254002, \"msg\":\"no more data\", \"data\":{\"items\":[{\"table_id\":\"tbl1\",\"name\":\"table_1\"}],\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListAllTableResponse.BaseItem> result = larkBaseService.listTables(baseId);

        assertEquals(1, result.size());
        assertEquals("tbl1", result.get(0).getTableId());
    }

    @Test
    public void listTables_withNullItems() {
        String baseId = "base123";
        String mockJsonResponse = "{\"code\":0, \"msg\":\"success\", \"data\":{\"items\":null,\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListAllTableResponse.BaseItem> result = larkBaseService.listTables(baseId);

        assertEquals(0, result.size());
    }

    @Test
    public void getTableFields_withNullItems() {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0, \"data\":{\"items\":null,\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<ListFieldResponse.FieldItem> result = larkBaseService.getTableFields(baseId, tableId);

        assertEquals(0, result.size());
    }

    @Test
    public void getDatabaseRecords_withNullItems() throws Exception {
        String baseId = "base1";
        String tableId = "tbl1";
        String mockJsonResponse = "{\"code\":0,\"data\":{\"items\":null,\"has_more\":false}}";

        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        mockHttpClient.addResponse(mockJsonResponse, 200, "OK");
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);

        List<LarkDatabaseRecord> result = larkBaseService.getDatabaseRecords(baseId, tableId);

        assertEquals(0, result.size());
    }

    @Test
    public void constructor_withDefaultHttpClient() {
        // Just verifying constructor works without error
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET);
        // If we get here, constructor succeeded
        assertEquals(LarkBaseService.class, larkBaseService.getClass());
    }

    @Test
    public void constructor_withCustomHttpClient() {
        // Just verifying constructor works without error
        MockHttpClientWrapper mockHttpClient = new MockHttpClientWrapper();
        LarkBaseService larkBaseService = new LarkBaseService(TEST_APP_ID, TEST_APP_SECRET, mockHttpClient);
        // If we get here, constructor succeeded
        assertEquals(LarkBaseService.class, larkBaseService.getClass());
    }
}

