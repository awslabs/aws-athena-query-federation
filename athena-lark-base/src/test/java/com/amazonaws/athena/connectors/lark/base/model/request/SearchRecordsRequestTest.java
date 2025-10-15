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
package com.amazonaws.athena.connectors.lark.base.model.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class SearchRecordsRequestTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testBuilder_minimalRequest() {
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .build();

        assertNotNull(request);
        assertEquals(Integer.valueOf(100), request.getPageSize());
        assertNull(request.getPageToken());
        assertNull(request.getFilter());
        assertNull(request.getSort());
    }

    @Test
    public void testBuilder_withPageToken() {
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(500)
            .pageToken("token123")
            .build();

        assertEquals(Integer.valueOf(500), Integer.valueOf(request.getPageSize()));
        assertEquals("token123", request.getPageToken());
    }

    @Test
    public void testBuilder_withFilter() {
        String filterJson = "{\"conjunction\":\"and\",\"conditions\":[{\"field_name\":\"field1\",\"operator\":\"is\",\"value\":[\"123\"]}]}";
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .filter(filterJson)
            .build();

        assertEquals(filterJson, request.getFilter());
    }

    @Test
    public void testBuilder_withSort() {
        String sortJson = "[{\"field_name\":\"field1\",\"desc\":true}]";
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .sort(sortJson)
            .build();

        assertEquals(sortJson, request.getSort());
    }

    @Test
    public void testBuilder_fullRequest() {
        String filterJson = "{\"conjunction\":\"and\",\"conditions\":[]}";
        String sortJson = "[{\"field_name\":\"field1\",\"desc\":false}]";

        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(250)
            .pageToken("page_2")
            .filter(filterJson)
            .sort(sortJson)
            .build();

        assertEquals(Integer.valueOf(250), Integer.valueOf(request.getPageSize()));
        assertEquals("page_2", request.getPageToken());
        assertEquals(filterJson, request.getFilter());
        assertEquals(sortJson, request.getSort());
    }

    @Test
    public void testJsonSerialization_withoutFilterAndSort() throws Exception {
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        assertEquals(100, jsonNode.get("page_size").asInt());
        assertFalse(jsonNode.has("page_token"));
        assertFalse(jsonNode.has("filter"));
        assertFalse(jsonNode.has("sort"));
    }

    @Test
    public void testJsonSerialization_withPageToken() throws Exception {
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .pageToken("token456")
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        assertEquals("token456", jsonNode.get("page_token").asText());
    }

    @Test
    public void testJsonSerialization_filterAsJsonObject() throws Exception {
        // Critical test: @JsonRawValue should serialize filter as JSON object, not escaped string
        String filterJson = "{\"conjunction\":\"and\",\"conditions\":[{\"field_name\":\"test\",\"operator\":\"is\",\"value\":[\"123\"]}]}";
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .filter(filterJson)
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        // Filter should be a JSON object, not a string
        assertTrue(jsonNode.get("filter").isObject());
        assertEquals("and", jsonNode.get("filter").get("conjunction").asText());
        assertTrue(jsonNode.get("filter").get("conditions").isArray());
    }

    @Test
    public void testJsonSerialization_sortAsJsonArray() throws Exception {
        // Critical test: @JsonRawValue should serialize sort as JSON array, not escaped string
        String sortJson = "[{\"field_name\":\"field1\",\"desc\":true},{\"field_name\":\"field2\",\"desc\":false}]";
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .sort(sortJson)
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        // Sort should be a JSON array, not a string
        assertTrue(jsonNode.get("sort").isArray());
        assertEquals(2, jsonNode.get("sort").size());
        assertEquals("field1", jsonNode.get("sort").get(0).get("field_name").asText());
        assertTrue(jsonNode.get("sort").get(0).get("desc").asBoolean());
    }

    @Test
    public void testJsonSerialization_fullRequest() throws Exception {
        String filterJson = "{\"conjunction\":\"and\",\"conditions\":[{\"field_name\":\"num\",\"operator\":\"isGreater\",\"value\":[\"100\"]}]}";
        String sortJson = "[{\"field_name\":\"num\",\"desc\":true}]";

        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(500)
            .pageToken("next_page")
            .filter(filterJson)
            .sort(sortJson)
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        assertEquals(500, jsonNode.get("page_size").asInt());
        assertEquals("next_page", jsonNode.get("page_token").asText());

        // Verify filter is JSON object
        assertTrue(jsonNode.get("filter").isObject());
        assertEquals("and", jsonNode.get("filter").get("conjunction").asText());

        // Verify sort is JSON array
        assertTrue(jsonNode.get("sort").isArray());
        assertEquals("num", jsonNode.get("sort").get(0).get("field_name").asText());
    }

    // Note: Empty filter string cannot be tested with @JsonRawValue as it tries to parse empty string as JSON
    // In practice, filter should either be null or valid JSON

    @Test
    public void testJsonSerialization_nullFilter() throws Exception {
        // Null filter should not be included in JSON
        SearchRecordsRequest request = SearchRecordsRequest.builder()
            .pageSize(100)
            .filter(null)
            .build();

        String json = OBJECT_MAPPER.writeValueAsString(request);
        JsonNode jsonNode = OBJECT_MAPPER.readTree(json);

        assertFalse(jsonNode.has("filter"));
    }

    // Note: SearchRecordsRequest doesn't override toString(), so no test for it
}
