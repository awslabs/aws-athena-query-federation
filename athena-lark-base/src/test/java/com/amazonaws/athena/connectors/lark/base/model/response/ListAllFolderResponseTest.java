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

class ListAllFolderResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDriveFileBuilder() {
        // Arrange & Act
        ListAllFolderResponse.DriveFile file = ListAllFolderResponse.DriveFile.builder()
                .name("My Folder")
                .parentToken("parent123")
                .token("token456")
                .type("folder")
                .build();

        // Assert
        assertThat(file.getName()).isEqualTo("my_folder"); // sanitized and lowercased
        assertThat(file.getRawName()).isEqualTo("My Folder");
        assertThat(file.getParentToken()).isEqualTo("parent123");
        assertThat(file.getToken()).isEqualTo("token456");
        assertThat(file.getType()).isEqualTo("folder");
    }

    @Test
    void testDriveFileNameSanitization() {
        // Arrange & Act
        ListAllFolderResponse.DriveFile file = ListAllFolderResponse.DriveFile.builder()
                .name("Test@Folder#123!")
                .token("tok1")
                .type("folder")
                .build();

        // Assert - CommonUtil.sanitizeGlueRelatedName should be applied
        assertThat(file.getName()).isNotEqualTo("Test@Folder#123!");
        assertThat(file.getRawName()).isEqualTo("Test@Folder#123!");
    }

    @Test
    void testListDataBuilder() {
        // Arrange
        ListAllFolderResponse.DriveFile file1 = ListAllFolderResponse.DriveFile.builder()
                .name("File1")
                .token("tok1")
                .type("file")
                .build();

        ListAllFolderResponse.DriveFile file2 = ListAllFolderResponse.DriveFile.builder()
                .name("File2")
                .token("tok2")
                .type("folder")
                .build();

        // Act
        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(List.of(file1, file2))
                .nextPageToken("next_token")
                .hasMore(true)
                .build();

        // Assert
        assertThat(listData.getFiles()).hasSize(2);
        assertThat(listData.getFiles().get(0).getToken()).isEqualTo("tok1");
        assertThat(listData.getFiles().get(1).getToken()).isEqualTo("tok2");
        assertThat(listData.getNextPageToken()).isEqualTo("next_token");
        assertThat(listData.hasMore()).isTrue();
    }

    @Test
    void testListDataWithEmptyFiles() {
        // Arrange & Act
        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(List.of())
                .nextPageToken(null)
                .hasMore(false)
                .build();

        // Assert
        assertThat(listData.getFiles()).isEmpty();
        assertThat(listData.getNextPageToken()).isNull();
        assertThat(listData.hasMore()).isFalse();
    }

    @Test
    void testListDataWithNullFiles() {
        // Arrange & Act
        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(null)
                .hasMore(false)
                .build();

        // Assert
        assertThat(listData.getFiles()).isEmpty();
    }

    @Test
    void testResponseBuilder() {
        // Arrange
        ListAllFolderResponse.DriveFile file = ListAllFolderResponse.DriveFile.builder()
                .name("TestFile")
                .token("test_token")
                .type("file")
                .build();

        ListAllFolderResponse.ListData data = ListAllFolderResponse.ListData.builder()
                .files(List.of(file))
                .nextPageToken("next123")
                .hasMore(true)
                .build();

        // Act
        ListAllFolderResponse.Builder builder = ListAllFolderResponse.builder();
        builder.data(data);
        ListAllFolderResponse response = builder.build();

        // Assert
        assertThat(response.getFiles()).hasSize(1);
        assertThat(response.getFiles().get(0).getToken()).isEqualTo("test_token");
        assertThat(response.getNextPageToken()).isEqualTo("next123");
        assertThat(response.hasMore()).isTrue();
    }

    @Test
    void testResponseWithNullData() {
        // Arrange & Act
        ListAllFolderResponse.Builder builder = ListAllFolderResponse.builder();
        builder.data(null);
        ListAllFolderResponse response = builder.build();

        // Assert
        assertThat(response.getFiles()).isEmpty();
        assertThat(response.getNextPageToken()).isNull();
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
                        "files": [
                            {
                                "name": "Documents",
                                "parent_token": "parent_tok",
                                "token": "file_token_1",
                                "type": "folder"
                            },
                            {
                                "name": "Spreadsheet",
                                "parent_token": "parent_tok",
                                "token": "file_token_2",
                                "type": "file"
                            }
                        ],
                        "next_page_token": "page_token_abc",
                        "has_more": true
                    }
                }
                """;

        // Act
        ListAllFolderResponse response = objectMapper.readValue(json, ListAllFolderResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isEqualTo("success");
        assertThat(response.getFiles()).hasSize(2);
        assertThat(response.getFiles().get(0).getName()).isEqualTo("documents"); // lowercased by sanitize
        assertThat(response.getFiles().get(0).getType()).isEqualTo("folder");
        assertThat(response.getFiles().get(1).getName()).isEqualTo("spreadsheet"); // lowercased by sanitize
        assertThat(response.getNextPageToken()).isEqualTo("page_token_abc");
        assertThat(response.hasMore()).isTrue();
    }

    @Test
    void testJsonDeserializationWithoutFiles() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "data": {
                        "next_page_token": null,
                        "has_more": false
                    }
                }
                """;

        // Act
        ListAllFolderResponse response = objectMapper.readValue(json, ListAllFolderResponse.class);

        // Assert
        assertThat(response.getFiles()).isEmpty();
        assertThat(response.getNextPageToken()).isNull();
        assertThat(response.hasMore()).isFalse();
    }

    @Test
    void testJsonDeserializationIgnoresUnknownFields() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "unknown_field": "ignored",
                    "data": {
                        "files": [
                            {
                                "name": "Test",
                                "token": "tok1",
                                "type": "file",
                                "extra_field": "should_ignore"
                            }
                        ],
                        "next_page_token": "next",
                        "has_more": false,
                        "another_unknown": 123
                    }
                }
                """;

        // Act
        ListAllFolderResponse response = objectMapper.readValue(json, ListAllFolderResponse.class);

        // Assert - should not throw exception
        assertThat(response.getFiles()).hasSize(1);
        assertThat(response.getFiles().get(0).getName()).isEqualTo("test"); // lowercased by sanitize
    }

    @Test
    void testDriveFileBuilderChaining() {
        // Arrange
        ListAllFolderResponse.DriveFile.Builder builder = ListAllFolderResponse.DriveFile.builder();

        // Act & Assert
        assertThat(builder.name("test")).isInstanceOf(ListAllFolderResponse.DriveFile.Builder.class);
        assertThat(builder.parentToken("parent")).isInstanceOf(ListAllFolderResponse.DriveFile.Builder.class);
        assertThat(builder.token("token")).isInstanceOf(ListAllFolderResponse.DriveFile.Builder.class);
        assertThat(builder.type("type")).isInstanceOf(ListAllFolderResponse.DriveFile.Builder.class);
    }

    @Test
    void testListDataBuilderChaining() {
        // Arrange
        ListAllFolderResponse.ListData.Builder builder = ListAllFolderResponse.ListData.builder();

        // Act & Assert
        assertThat(builder.files(List.of())).isInstanceOf(ListAllFolderResponse.ListData.Builder.class);
        assertThat(builder.nextPageToken("token")).isInstanceOf(ListAllFolderResponse.ListData.Builder.class);
        assertThat(builder.hasMore(true)).isInstanceOf(ListAllFolderResponse.ListData.Builder.class);
    }

    @Test
    void testResponseWithMultiplePages() {
        // Arrange
        List<ListAllFolderResponse.DriveFile> files = List.of(
                ListAllFolderResponse.DriveFile.builder().name("File1").token("t1").type("file").build(),
                ListAllFolderResponse.DriveFile.builder().name("File2").token("t2").type("file").build(),
                ListAllFolderResponse.DriveFile.builder().name("File3").token("t3").type("folder").build()
        );

        ListAllFolderResponse.ListData data = ListAllFolderResponse.ListData.builder()
                .files(files)
                .nextPageToken("has_next_page")
                .hasMore(true)
                .build();

        // Act
        ListAllFolderResponse.Builder builder = ListAllFolderResponse.builder();
        builder.data(data);
        ListAllFolderResponse response = builder.build();

        // Assert
        assertThat(response.getFiles()).hasSize(3);
        assertThat(response.hasMore()).isTrue();
        assertThat(response.getNextPageToken()).isNotNull();
    }

    @Test
    void testListDataImmutability() {
        // Arrange
        ListAllFolderResponse.DriveFile file = ListAllFolderResponse.DriveFile.builder()
                .name("File").token("tok").type("file").build();

        java.util.ArrayList<ListAllFolderResponse.DriveFile> mutableList = new java.util.ArrayList<>();
        mutableList.add(file);

        ListAllFolderResponse.ListData listData = ListAllFolderResponse.ListData.builder()
                .files(mutableList)
                .build();

        // Act - try to modify original list
        mutableList.add(ListAllFolderResponse.DriveFile.builder().name("File2").token("tok2").type("file").build());

        // Assert - listData should not be affected
        assertThat(listData.getFiles()).hasSize(1);
    }
}
