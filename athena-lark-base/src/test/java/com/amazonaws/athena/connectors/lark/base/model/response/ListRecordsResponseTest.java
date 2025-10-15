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
package com.amazonaws.athena.connectors.lark.base.model.response;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ListRecordsResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testRecordItemBuilder() {
        // Arrange & Act
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec123")
                .fields(Map.of("name", "John", "age", 30))
                .build();

        // Assert
        assertThat(item.getRecordId()).isEqualTo("rec123");
        assertThat(item.getFields()).hasSize(2);
        assertThat(item.getFields().get("name")).isEqualTo("John");
        assertThat(item.getFields().get("age")).isEqualTo(30);
    }

    @Test
    void testRecordItemWithNullFields() {
        // Arrange & Act
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(null)
                .build();

        // Assert
        assertThat(item.getRecordId()).isEqualTo("rec1");
        assertThat(item.getFields()).isEmpty();
    }

    @Test
    void testRecordItemFiltersNullValues() {
        // Arrange
        Map<String, Object> fieldsWithNull = new HashMap<>();
        fieldsWithNull.put("name", "Alice");
        fieldsWithNull.put("email", null);
        fieldsWithNull.put("age", 25);

        // Act
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec2")
                .fields(fieldsWithNull)
                .build();

        // Assert - null values should be filtered out
        assertThat(item.getFields()).hasSize(2);
        assertThat(item.getFields()).containsKeys("name", "age");
        assertThat(item.getFields()).doesNotContainKey("email");
    }

    @Test
    void testRecordItemSetFields() {
        // Arrange
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("old", "value"))
                .build();

        // Act
        item.setFields(Map.of("new", "data"));

        // Assert
        assertThat(item.getFields()).hasSize(1);
        assertThat(item.getFields().get("new")).isEqualTo("data");
        assertThat(item.getFields()).doesNotContainKey("old");
    }

    @Test
    void testRecordItemSetFieldsWithNull() {
        // Arrange
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("key", "value"))
                .build();

        // Act
        item.setFields(null);

        // Assert
        assertThat(item.getFields()).isEmpty();
    }

    @Test
    void testRecordItemFieldsImmutability() {
        // Arrange
        HashMap<String, Object> mutableMap = new HashMap<>();
        mutableMap.put("key1", "value1");

        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(mutableMap)
                .build();

        // Act - try to modify original map
        mutableMap.put("key2", "value2");

        // Assert - item's fields should not be affected (it's a copy)
        assertThat(item.getFields()).hasSize(1);
        assertThat(item.getFields()).containsOnlyKeys("key1");
    }

    @Test
    void testListDataBuilder() {
        // Arrange
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("name", "John"))
                .build();

        ListRecordsResponse.RecordItem item2 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec2")
                .fields(Map.of("name", "Jane"))
                .build();

        // Act
        ListRecordsResponse.ListData listData = ListRecordsResponse.ListData.builder()
                .items(List.of(item1, item2))
                .pageToken("next_token")
                .hasMore(true)
                .total(100)
                .build();

        // Assert
        assertThat(listData.items()).hasSize(2);
        assertThat(listData.pageToken()).isEqualTo("next_token");
        assertThat(listData.hasMore()).isTrue();
        assertThat(listData.total()).isEqualTo(100);
    }

    @Test
    void testListDataWithNullItems() {
        // Arrange & Act
        ListRecordsResponse.ListData listData = ListRecordsResponse.ListData.builder()
                .items(null)
                .pageToken(null)
                .hasMore(false)
                .total(0)
                .build();

        // Assert
        assertThat(listData.items()).isEmpty();
        assertThat(listData.pageToken()).isNull();
        assertThat(listData.hasMore()).isFalse();
        assertThat(listData.total()).isEqualTo(0);
    }

    @Test
    void testResponseBuilder() {
        // Arrange
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("field1", "value1"))
                .build();

        ListRecordsResponse.ListData data = ListRecordsResponse.ListData.builder()
                .items(List.of(item))
                .pageToken("token123")
                .hasMore(true)
                .total(50)
                .build();

        // Act
        ListRecordsResponse.Builder builder = ListRecordsResponse.builder();
        builder.data(data);
        ListRecordsResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).hasSize(1);
        assertThat(response.getPageToken()).isEqualTo("token123");
        assertThat(response.hasMore()).isTrue();
        assertThat(response.getTotal()).isEqualTo(50);
    }

    @Test
    void testResponseWithNullData() {
        // Arrange & Act
        ListRecordsResponse.Builder builder = ListRecordsResponse.builder();
        builder.data(null);
        ListRecordsResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).isEmpty();
        assertThat(response.getPageToken()).isNull();
        assertThat(response.hasMore()).isFalse();
        assertThat(response.getTotal()).isEqualTo(0);
    }

    @Test
    void testJsonDeserializationComplete() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "items": [
                            {
                                "record_id": "rec001",
                                "fields": {
                                    "name": "Alice",
                                    "age": 30,
                                    "city": "NYC"
                                }
                            },
                            {
                                "record_id": "rec002",
                                "fields": {
                                    "name": "Bob",
                                    "age": 25
                                }
                            }
                        ],
                        "page_token": "page_abc",
                        "has_more": true,
                        "total": 200
                    }
                }
                """;

        // Act
        ListRecordsResponse response = objectMapper.readValue(json, ListRecordsResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getItems()).hasSize(2);
        assertThat(response.getItems().get(0).getRecordId()).isEqualTo("rec001");
        assertThat(response.getItems().get(0).getFields().get("name")).isEqualTo("Alice");
        assertThat(response.getItems().get(0).getFields().get("age")).isEqualTo(30);
        assertThat(response.getItems().get(1).getRecordId()).isEqualTo("rec002");
        assertThat(response.getPageToken()).isEqualTo("page_abc");
        assertThat(response.hasMore()).isTrue();
        assertThat(response.getTotal()).isEqualTo(200);
    }

    @Test
    void testJsonDeserializationWithEmptyItems() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "data": {
                        "items": [],
                        "page_token": null,
                        "has_more": false,
                        "total": 0
                    }
                }
                """;

        // Act
        ListRecordsResponse response = objectMapper.readValue(json, ListRecordsResponse.class);

        // Assert
        assertThat(response.getItems()).isEmpty();
        assertThat(response.getPageToken()).isNull();
        assertThat(response.hasMore()).isFalse();
        assertThat(response.getTotal()).isEqualTo(0);
    }

    @Test
    void testJsonDeserializationWithNestedObjects() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "data": {
                        "items": [
                            {
                                "record_id": "rec1",
                                "fields": {
                                    "user": {
                                        "name": "Charlie",
                                        "id": 123
                                    },
                                    "tags": ["tag1", "tag2"]
                                }
                            }
                        ],
                        "has_more": false,
                        "total": 1
                    }
                }
                """;

        // Act
        ListRecordsResponse response = objectMapper.readValue(json, ListRecordsResponse.class);

        // Assert
        assertThat(response.getItems()).hasSize(1);
        assertThat(response.getItems().get(0).getFields()).containsKeys("user", "tags");
    }

    @Test
    void testRecordItemBuilderChaining() {
        // Arrange
        ListRecordsResponse.RecordItem.Builder builder = ListRecordsResponse.RecordItem.builder();

        // Act & Assert
        assertThat(builder.recordId("id")).isInstanceOf(ListRecordsResponse.RecordItem.Builder.class);
        assertThat(builder.fields(Map.of())).isInstanceOf(ListRecordsResponse.RecordItem.Builder.class);
    }

    @Test
    void testListDataBuilderChaining() {
        // Arrange
        ListRecordsResponse.ListData.Builder builder = ListRecordsResponse.ListData.builder();

        // Act & Assert
        assertThat(builder.items(List.of())).isInstanceOf(ListRecordsResponse.ListData.Builder.class);
        assertThat(builder.pageToken("token")).isInstanceOf(ListRecordsResponse.ListData.Builder.class);
        assertThat(builder.hasMore(true)).isInstanceOf(ListRecordsResponse.ListData.Builder.class);
        assertThat(builder.total(10)).isInstanceOf(ListRecordsResponse.ListData.Builder.class);
    }

    @Test
    void testListDataImmutability() {
        // Arrange
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("key", "value"))
                .build();

        java.util.ArrayList<ListRecordsResponse.RecordItem> mutableList = new java.util.ArrayList<>();
        mutableList.add(item);

        ListRecordsResponse.ListData listData = ListRecordsResponse.ListData.builder()
                .items(mutableList)
                .build();

        // Act - try to modify original list
        mutableList.add(ListRecordsResponse.RecordItem.builder().recordId("rec2").build());

        // Assert - listData should not be affected
        assertThat(listData.items()).hasSize(1);
    }

    @Test
    void testRecordItemWithComplexFields() {
        // Arrange
        Map<String, Object> complexFields = new HashMap<>();
        complexFields.put("string", "text");
        complexFields.put("number", 42);
        complexFields.put("boolean", true);
        complexFields.put("list", List.of(1, 2, 3));
        complexFields.put("map", Map.of("nested", "value"));

        // Act
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(complexFields)
                .build();

        // Assert
        assertThat(item.getFields()).hasSize(5);
        assertThat(item.getFields().get("string")).isEqualTo("text");
        assertThat(item.getFields().get("number")).isEqualTo(42);
        assertThat(item.getFields().get("boolean")).isEqualTo(true);
        assertThat(item.getFields().get("list")).isInstanceOf(List.class);
        assertThat(item.getFields().get("map")).isInstanceOf(Map.class);
    }

    @Test
    void testSetFieldsCreatesNewMap() {
        // Arrange
        ListRecordsResponse.RecordItem item = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("initial", "value"))
                .build();

        HashMap<String, Object> newFields = new HashMap<>();
        newFields.put("updated", "data");

        // Act
        item.setFields(newFields);
        newFields.put("modified", "after_set");

        // Assert - setFields creates a new HashMap at the time of call, so only "updated" is in item
        assertThat(item.getFields()).hasSize(1);
        assertThat(item.getFields()).containsKey("updated");
        assertThat(item.getFields()).doesNotContainKey("modified");
    }

    @Test
    void testMultipleRecordsWithDifferentFields() {
        // Arrange & Act
        ListRecordsResponse.RecordItem item1 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec1")
                .fields(Map.of("name", "Alice", "age", 30))
                .build();

        ListRecordsResponse.RecordItem item2 = ListRecordsResponse.RecordItem.builder()
                .recordId("rec2")
                .fields(Map.of("name", "Bob", "city", "NYC"))
                .build();

        ListRecordsResponse.ListData data = ListRecordsResponse.ListData.builder()
                .items(List.of(item1, item2))
                .hasMore(false)
                .total(2)
                .build();

        ListRecordsResponse.Builder builder = ListRecordsResponse.builder();
        builder.data(data);
        ListRecordsResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).hasSize(2);
        assertThat(response.getItems().get(0).getFields()).containsKeys("name", "age");
        assertThat(response.getItems().get(1).getFields()).containsKeys("name", "city");
    }
}
