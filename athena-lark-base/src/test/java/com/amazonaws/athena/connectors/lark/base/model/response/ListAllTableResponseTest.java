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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ListAllTableResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testBaseItemBuilder() {
        // Arrange & Act
        ListAllTableResponse.BaseItem item = ListAllTableResponse.BaseItem.builder()
                .tableId("tbl123")
                .revision("rev456")
                .name("My Table")
                .build();

        // Assert
        assertThat(item.getTableId()).isEqualTo("tbl123");
        assertThat(item.getRevision()).isEqualTo("rev456");
        assertThat(item.getName()).isEqualTo("my_table"); // sanitized and lowercased
        assertThat(item.getRawName()).isEqualTo("My Table");
    }

    @Test
    void testBaseItemNameSanitization() {
        // Arrange & Act
        ListAllTableResponse.BaseItem item = ListAllTableResponse.BaseItem.builder()
                .tableId("tbl1")
                .name("Test@Table#123!")
                .build();

        // Assert - CommonUtil.sanitizeGlueRelatedName should be applied
        assertThat(item.getName()).isNotEqualTo("Test@Table#123!");
        assertThat(item.getRawName()).isEqualTo("Test@Table#123!");
    }

    @Test
    void testBaseItemWithNullValues() {
        // Arrange & Act
        ListAllTableResponse.BaseItem item = ListAllTableResponse.BaseItem.builder()
                .tableId(null)
                .revision(null)
                .name(null)
                .build();

        // Assert
        assertThat(item.getTableId()).isNull();
        assertThat(item.getRevision()).isNull();
        assertThat(item.getRawName()).isNull();
    }

    @Test
    void testListDataBuilder() {
        // Arrange
        ListAllTableResponse.BaseItem item1 = ListAllTableResponse.BaseItem.builder()
                .tableId("tbl1")
                .name("Table1")
                .revision("rev1")
                .build();

        ListAllTableResponse.BaseItem item2 = ListAllTableResponse.BaseItem.builder()
                .tableId("tbl2")
                .name("Table2")
                .revision("rev2")
                .build();

        // Act
        ListAllTableResponse.ListData listData = ListAllTableResponse.ListData.builder()
                .items(List.of(item1, item2))
                .pageToken("next_token")
                .hasMore(true)
                .build();

        // Assert
        assertThat(listData.getItems()).hasSize(2);
        assertThat(listData.getItems().get(0).getTableId()).isEqualTo("tbl1");
        assertThat(listData.getItems().get(1).getTableId()).isEqualTo("tbl2");
        assertThat(listData.getPageToken()).isEqualTo("next_token");
        assertThat(listData.hasMore()).isTrue();
    }

    @Test
    void testListDataWithEmptyItems() {
        // Arrange & Act
        ListAllTableResponse.ListData listData = ListAllTableResponse.ListData.builder()
                .items(List.of())
                .pageToken(null)
                .hasMore(false)
                .build();

        // Assert
        assertThat(listData.getItems()).isEmpty();
        assertThat(listData.getPageToken()).isNull();
        assertThat(listData.hasMore()).isFalse();
    }

    @Test
    void testListDataWithNullItems() {
        // Arrange & Act
        ListAllTableResponse.ListData listData = ListAllTableResponse.ListData.builder()
                .items(null)
                .hasMore(false)
                .build();

        // Assert
        assertThat(listData.getItems()).isEmpty();
    }

    @Test
    void testResponseBuilder() {
        // Arrange
        ListAllTableResponse.BaseItem item = ListAllTableResponse.BaseItem.builder()
                .tableId("test_tbl")
                .name("TestTable")
                .revision("rev1")
                .build();

        ListAllTableResponse.ListData data = ListAllTableResponse.ListData.builder()
                .items(List.of(item))
                .pageToken("token123")
                .hasMore(true)
                .build();

        // Act
        ListAllTableResponse.Builder builder = ListAllTableResponse.builder();
        builder.data(data);
        ListAllTableResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).hasSize(1);
        assertThat(response.getItems().get(0).getTableId()).isEqualTo("test_tbl");
        assertThat(response.getPageToken()).isEqualTo("token123");
        assertThat(response.hasMore()).isTrue();
    }

    @Test
    void testResponseWithNullData() {
        // Arrange & Act
        ListAllTableResponse.Builder builder = ListAllTableResponse.builder();
        builder.data(null);
        ListAllTableResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).isEmpty();
        assertThat(response.getPageToken()).isNull();
        assertThat(response.hasMore()).isFalse();
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
                                "table_id": "tblxxx1",
                                "revision": "1",
                                "name": "Sales Data"
                            },
                            {
                                "table_id": "tblxxx2",
                                "revision": "2",
                                "name": "Customer Info"
                            }
                        ],
                        "page_token": "page_abc",
                        "has_more": true
                    }
                }
                """;

        // Act
        ListAllTableResponse response = objectMapper.readValue(json, ListAllTableResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getItems()).hasSize(2);
        assertThat(response.getItems().get(0).getTableId()).isEqualTo("tblxxx1");
        assertThat(response.getItems().get(0).getName()).isEqualTo("sales_data"); // lowercased by sanitize
        assertThat(response.getItems().get(0).getRevision()).isEqualTo("1");
        assertThat(response.getItems().get(1).getTableId()).isEqualTo("tblxxx2");
        assertThat(response.getItems().get(1).getName()).isEqualTo("customer_info"); // lowercased by sanitize
        assertThat(response.getPageToken()).isEqualTo("page_abc");
        assertThat(response.hasMore()).isTrue();
    }

    @Test
    void testJsonDeserializationWithoutItems() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "page_token": null,
                        "has_more": false
                    }
                }
                """;

        // Act
        ListAllTableResponse response = objectMapper.readValue(json, ListAllTableResponse.class);

        // Assert
        assertThat(response.getItems()).isEmpty();
        assertThat(response.getPageToken()).isNull();
        assertThat(response.hasMore()).isFalse();
    }

    @Test
    void testJsonDeserializationIgnoresUnknownFieldsAtTopLevel() throws Exception {
        // Arrange - unknown fields at response and data level only
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "unknown_field": "ignored",
                    "data": {
                        "items": [
                            {
                                "table_id": "tbl1",
                                "name": "Test",
                                "revision": "1"
                            }
                        ],
                        "page_token": "next",
                        "has_more": false,
                        "another_unknown": 123
                    }
                }
                """;

        // Act
        ListAllTableResponse response = objectMapper.readValue(json, ListAllTableResponse.class);

        // Assert - should not throw exception
        assertThat(response.getItems()).hasSize(1);
        assertThat(response.getItems().get(0).getName()).isEqualTo("test"); // lowercased by sanitize
    }

    @Test
    void testBaseItemBuilderChaining() {
        // Arrange
        ListAllTableResponse.BaseItem.Builder builder = ListAllTableResponse.BaseItem.builder();

        // Act & Assert
        assertThat(builder.tableId("id")).isInstanceOf(ListAllTableResponse.BaseItem.Builder.class);
        assertThat(builder.revision("rev")).isInstanceOf(ListAllTableResponse.BaseItem.Builder.class);
        assertThat(builder.name("name")).isInstanceOf(ListAllTableResponse.BaseItem.Builder.class);
    }

    @Test
    void testListDataBuilderChaining() {
        // Arrange
        ListAllTableResponse.ListData.Builder builder = ListAllTableResponse.ListData.builder();

        // Act & Assert
        assertThat(builder.items(List.of())).isInstanceOf(ListAllTableResponse.ListData.Builder.class);
        assertThat(builder.pageToken("token")).isInstanceOf(ListAllTableResponse.ListData.Builder.class);
        assertThat(builder.hasMore(true)).isInstanceOf(ListAllTableResponse.ListData.Builder.class);
    }

    @Test
    void testResponseWithMultiplePages() {
        // Arrange
        List<ListAllTableResponse.BaseItem> items = List.of(
                ListAllTableResponse.BaseItem.builder().tableId("t1").name("Table1").revision("r1").build(),
                ListAllTableResponse.BaseItem.builder().tableId("t2").name("Table2").revision("r2").build(),
                ListAllTableResponse.BaseItem.builder().tableId("t3").name("Table3").revision("r3").build()
        );

        ListAllTableResponse.ListData data = ListAllTableResponse.ListData.builder()
                .items(items)
                .pageToken("has_next_page")
                .hasMore(true)
                .build();

        // Act
        ListAllTableResponse.Builder builder = ListAllTableResponse.builder();
        builder.data(data);
        ListAllTableResponse response = builder.build();

        // Assert
        assertThat(response.getItems()).hasSize(3);
        assertThat(response.hasMore()).isTrue();
        assertThat(response.getPageToken()).isNotNull();
    }

    @Test
    void testListDataImmutability() {
        // Arrange
        ListAllTableResponse.BaseItem item = ListAllTableResponse.BaseItem.builder()
                .tableId("tbl1").name("Table").revision("rev").build();

        java.util.ArrayList<ListAllTableResponse.BaseItem> mutableList = new java.util.ArrayList<>();
        mutableList.add(item);

        ListAllTableResponse.ListData listData = ListAllTableResponse.ListData.builder()
                .items(mutableList)
                .build();

        // Act - try to modify original list
        mutableList.add(ListAllTableResponse.BaseItem.builder().tableId("tbl2").name("Table2").revision("rev2").build());

        // Assert - listData should not be affected
        assertThat(listData.getItems()).hasSize(1);
    }
}
