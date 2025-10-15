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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableRecordsRequestTest {

    @Test
    void testBuilderWithAllFields() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_123")
                .tableId("tbl_456")
                .pageSize(100)
                .pageToken("page_abc")
                .filterJson("{\"conditions\":[{\"field_name\":\"Name\",\"operator\":\"is\",\"value\":\"John\"}]}")
                .sortJson("[{\"field_name\":\"Age\",\"desc\":false}]")
                .build();

        // Assert
        assertThat(request.getBaseId()).isEqualTo("base_123");
        assertThat(request.getTableId()).isEqualTo("tbl_456");
        assertThat(request.getPageSize()).isEqualTo(100);
        assertThat(request.getPageToken()).isEqualTo("page_abc");
        assertThat(request.getFilterJson()).contains("Name");
        assertThat(request.getSortJson()).contains("Age");
    }

    @Test
    void testBuilderWithRequiredFieldsOnly() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_xyz")
                .tableId("tbl_xyz")
                .build();

        // Assert
        assertThat(request.getBaseId()).isEqualTo("base_xyz");
        assertThat(request.getTableId()).isEqualTo("tbl_xyz");
        assertThat(request.getPageSize()).isEqualTo(500); // default value
        assertThat(request.getPageToken()).isNull();
        assertThat(request.getFilterJson()).isNull();
        assertThat(request.getSortJson()).isNull();
    }

    @Test
    void testBuilderThrowsWhenBaseIdIsNull() {
        // Assert
        assertThatThrownBy(() ->
                TableRecordsRequest.builder()
                        .tableId("tbl_123")
                        .build()
        )
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("baseId cannot be null");
    }

    @Test
    void testBuilderThrowsWhenTableIdIsNull() {
        // Assert
        assertThatThrownBy(() ->
                TableRecordsRequest.builder()
                        .baseId("base_123")
                        .build()
        )
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("tableId cannot be null");
    }

    @Test
    void testBuilderThrowsWhenBothIdsAreNull() {
        // Assert
        assertThatThrownBy(() ->
                TableRecordsRequest.builder()
                        .build()
        )
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testBuilderWithCustomPageSize() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_001")
                .tableId("tbl_001")
                .pageSize(1000)
                .build();

        // Assert
        assertThat(request.getPageSize()).isEqualTo(1000);
    }

    @Test
    void testBuilderWithPageToken() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_002")
                .tableId("tbl_002")
                .pageToken("next_page_token_xyz")
                .build();

        // Assert
        assertThat(request.getPageToken()).isEqualTo("next_page_token_xyz");
    }

    @Test
    void testBuilderWithFilterJson() {
        // Arrange
        String filterJson = "{\"conjunction\":\"and\",\"conditions\":[{\"field_name\":\"Status\",\"operator\":\"is\",\"value\":\"Active\"}]}";

        // Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_003")
                .tableId("tbl_003")
                .filterJson(filterJson)
                .build();

        // Assert
        assertThat(request.getFilterJson()).isEqualTo(filterJson);
    }

    @Test
    void testBuilderWithSortJson() {
        // Arrange
        String sortJson = "[{\"field_name\":\"CreatedAt\",\"desc\":true}]";

        // Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_004")
                .tableId("tbl_004")
                .sortJson(sortJson)
                .build();

        // Assert
        assertThat(request.getSortJson()).isEqualTo(sortJson);
    }

    @Test
    void testBuilderChaining() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_chain")
                .tableId("tbl_chain")
                .pageSize(250)
                .pageToken("token_chain")
                .filterJson("{}")
                .sortJson("[]")
                .build();

        // Assert
        assertThat(request.getBaseId()).isEqualTo("base_chain");
        assertThat(request.getTableId()).isEqualTo("tbl_chain");
        assertThat(request.getPageSize()).isEqualTo(250);
        assertThat(request.getPageToken()).isEqualTo("token_chain");
        assertThat(request.getFilterJson()).isEqualTo("{}");
        assertThat(request.getSortJson()).isEqualTo("[]");
    }

    @Test
    void testBuilderReturnsBuilder() {
        // Arrange
        TableRecordsRequest.Builder builder = TableRecordsRequest.builder();

        // Act & Assert
        assertThat(builder.baseId("base")).isInstanceOf(TableRecordsRequest.Builder.class);
        assertThat(builder.tableId("tbl")).isInstanceOf(TableRecordsRequest.Builder.class);
        assertThat(builder.pageSize(100)).isInstanceOf(TableRecordsRequest.Builder.class);
        assertThat(builder.pageToken("token")).isInstanceOf(TableRecordsRequest.Builder.class);
        assertThat(builder.filterJson("{}")).isInstanceOf(TableRecordsRequest.Builder.class);
        assertThat(builder.sortJson("[]")).isInstanceOf(TableRecordsRequest.Builder.class);
    }

    @Test
    void testDefaultPageSize() {
        // Arrange & Act
        TableRecordsRequest request = TableRecordsRequest.builder()
                .baseId("base_default")
                .tableId("tbl_default")
                .build();

        // Assert - default should be 500
        assertThat(request.getPageSize()).isEqualTo(500);
    }

    @Test
    void testMultipleBuildsFromSameBuilder() {
        // Arrange
        TableRecordsRequest.Builder builder = TableRecordsRequest.builder()
                .baseId("base_multi")
                .tableId("tbl_multi");

        // Act
        TableRecordsRequest request1 = builder.build();
        TableRecordsRequest request2 = builder.pageSize(300).build();

        // Assert - both requests should have the same base and table IDs
        assertThat(request1.getBaseId()).isEqualTo("base_multi");
        assertThat(request1.getTableId()).isEqualTo("tbl_multi");
        assertThat(request1.getPageSize()).isEqualTo(500); // default

        assertThat(request2.getBaseId()).isEqualTo("base_multi");
        assertThat(request2.getTableId()).isEqualTo("tbl_multi");
        assertThat(request2.getPageSize()).isEqualTo(300); // modified
    }
}
