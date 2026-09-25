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

import static org.assertj.core.api.Assertions.assertThat;

class BaseResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testBuilderWithStringData() {
        // Arrange & Act
        BaseResponse<String> response = BaseResponse.<String>builder()
                .data("test data")
                .build();

        // Assert
        assertThat(response.getCode()).isEqualTo(0); // default value
        assertThat(response.getMsg()).isNull(); // default value
        assertThat(response.getData()).isEqualTo("test data");
    }

    @Test
    void testBuilderWithIntegerData() {
        // Arrange & Act
        BaseResponse<Integer> response = BaseResponse.<Integer>builder()
                .data(42)
                .build();

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isNull();
        assertThat(response.getData()).isEqualTo(42);
    }

    @Test
    void testBuilderWithNullData() {
        // Arrange & Act
        BaseResponse<String> response = BaseResponse.<String>builder()
                .data(null)
                .build();

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isNull();
        assertThat(response.getData()).isNull();
    }

    @Test
    void testBuilderWithoutData() {
        // Arrange & Act
        BaseResponse<String> response = BaseResponse.<String>builder()
                .build();

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isNull();
        assertThat(response.getData()).isNull();
    }

    @Test
    void testBuilderWithComplexData() {
        // Arrange
        TestData testData = new TestData("name", 123);

        // Act
        BaseResponse<TestData> response = BaseResponse.<TestData>builder()
                .data(testData)
                .build();

        // Assert
        assertThat(response.getData()).isNotNull();
        assertThat(response.getData().name).isEqualTo("name");
        assertThat(response.getData().value).isEqualTo(123);
    }

    @Test
    void testJsonDeserializationWithStringData() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "data": "test string"
                }
                """;

        // Act
        BaseResponse<?> response = objectMapper.readValue(json, BaseResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isEqualTo("success");
        assertThat(response.getData()).isEqualTo("test string");
    }

    @Test
    void testJsonDeserializationWithNullData() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "data": null
                }
                """;

        // Act
        BaseResponse<?> response = objectMapper.readValue(json, BaseResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isEqualTo("success");
        assertThat(response.getData()).isNull();
    }

    @Test
    void testJsonDeserializationWithoutDataField() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 1,
                    "msg": "error"
                }
                """;

        // Act
        BaseResponse<?> response = objectMapper.readValue(json, BaseResponse.class);

        // Assert
        assertThat(response.getCode()).isEqualTo(1);
        assertThat(response.getMsg()).isEqualTo("error");
        assertThat(response.getData()).isNull();
    }

    @Test
    void testJsonDeserializationIgnoresUnknownFields() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "ok",
                    "data": "value",
                    "unknown_field": "should be ignored",
                    "another_unknown": 123
                }
                """;

        // Act
        BaseResponse<?> response = objectMapper.readValue(json, BaseResponse.class);

        // Assert - should not throw exception
        assertThat(response.getCode()).isEqualTo(0);
        assertThat(response.getMsg()).isEqualTo("ok");
        assertThat(response.getData()).isEqualTo("value");
    }

    @Test
    void testBuilderReturnsBuilder() {
        // Arrange
        BaseResponse.Builder<String> builder = BaseResponse.<String>builder();

        // Act & Assert
        assertThat(builder.data("test")).isInstanceOf(BaseResponse.Builder.class);
    }

    @Test
    void testMultipleBuildsFromSameBuilder() {
        // Arrange
        BaseResponse.Builder<String> builder = BaseResponse.<String>builder();

        // Act
        BaseResponse<String> response1 = builder.data("first").build();
        BaseResponse<String> response2 = builder.data("second").build();

        // Assert
        assertThat(response1.getData()).isEqualTo("first");
        assertThat(response2.getData()).isEqualTo("second");
    }

    // Helper class for testing complex data
    private static class TestData {
        String name;
        int value;

        TestData(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
