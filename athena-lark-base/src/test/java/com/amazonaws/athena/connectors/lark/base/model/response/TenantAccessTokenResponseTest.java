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

class TenantAccessTokenResponseTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testConstructorAndAccessors() {
        // Arrange & Act
        TenantAccessTokenResponse response = new TenantAccessTokenResponse(
                0,
                "success",
                7200,
                "t-abc123xyz789"
        );

        // Assert
        assertThat(response.code()).isEqualTo(0);
        assertThat(response.msg()).isEqualTo("success");
        assertThat(response.expire()).isEqualTo(7200);
        assertThat(response.tenantAccessToken()).isEqualTo("t-abc123xyz789");
    }

    @Test
    void testRecordEquality() {
        // Arrange
        TenantAccessTokenResponse response1 = new TenantAccessTokenResponse(0, "ok", 3600, "token123");
        TenantAccessTokenResponse response2 = new TenantAccessTokenResponse(0, "ok", 3600, "token123");
        TenantAccessTokenResponse response3 = new TenantAccessTokenResponse(1, "error", 0, null);

        // Assert
        assertThat(response1).isEqualTo(response2);
        assertThat(response1.hashCode()).isEqualTo(response2.hashCode());
        assertThat(response1).isNotEqualTo(response3);
    }

    @Test
    void testRecordToString() {
        // Arrange
        TenantAccessTokenResponse response = new TenantAccessTokenResponse(
                0, "success", 7200, "my-token"
        );

        // Act
        String toString = response.toString();

        // Assert
        assertThat(toString).contains("TenantAccessTokenResponse");
        assertThat(toString).contains("0");
        assertThat(toString).contains("success");
        assertThat(toString).contains("7200");
        assertThat(toString).contains("my-token");
    }

    @Test
    void testJsonDeserialization() throws Exception {
        // Arrange
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "expire": 7200,
                    "tenant_access_token": "t-g1044ghJRGBDHYDIRWKOMDYDZ3VTN3VUQYE5RZI"
                }
                """;

        // Act
        TenantAccessTokenResponse response = objectMapper.readValue(json, TenantAccessTokenResponse.class);

        // Assert
        assertThat(response.code()).isEqualTo(0);
        assertThat(response.msg()).isEqualTo("success");
        assertThat(response.expire()).isEqualTo(7200);
        assertThat(response.tenantAccessToken()).isEqualTo("t-g1044ghJRGBDHYDIRWKOMDYDZ3VTN3VUQYE5RZI");
    }

    @Test
    void testJsonSerialization() throws Exception {
        // Arrange
        TenantAccessTokenResponse response = new TenantAccessTokenResponse(
                0, "success", 3600, "token-xyz"
        );

        // Act
        String json = objectMapper.writeValueAsString(response);

        // Assert
        assertThat(json).contains("\"code\":0");
        assertThat(json).contains("\"msg\":\"success\"");
        assertThat(json).contains("\"expire\":3600");
        assertThat(json).contains("\"tenant_access_token\":\"token-xyz\"");
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        // Arrange
        TenantAccessTokenResponse original = new TenantAccessTokenResponse(
                0, "ok", 5000, "round-trip-token"
        );

        // Act
        String json = objectMapper.writeValueAsString(original);
        TenantAccessTokenResponse deserialized = objectMapper.readValue(json, TenantAccessTokenResponse.class);

        // Assert
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    void testErrorResponse() {
        // Arrange & Act
        TenantAccessTokenResponse response = new TenantAccessTokenResponse(
                99991663,
                "app ticket is not valid",
                0,
                ""
        );

        // Assert
        assertThat(response.code()).isNotEqualTo(0);
        assertThat(response.msg()).contains("not valid");
        assertThat(response.expire()).isEqualTo(0);
        assertThat(response.tenantAccessToken()).isEmpty();
    }

    @Test
    void testWithNullValues() {
        // Arrange & Act
        TenantAccessTokenResponse response = new TenantAccessTokenResponse(
                0, null, 0, null
        );

        // Assert
        assertThat(response.code()).isEqualTo(0);
        assertThat(response.msg()).isNull();
        assertThat(response.expire()).isEqualTo(0);
        assertThat(response.tenantAccessToken()).isNull();
    }

    @Test
    void testJsonDeserializationWithUnknownFields() throws Exception {
        // Arrange - JSON with extra fields that should be ignored
        String json = """
                {
                    "code": 0,
                    "msg": "success",
                    "expire": 7200,
                    "tenant_access_token": "token123",
                    "extra_field": "should_be_ignored",
                    "another_field": 999
                }
                """;

        // Act
        TenantAccessTokenResponse response = objectMapper.readValue(json, TenantAccessTokenResponse.class);

        // Assert - extra fields should not cause errors
        assertThat(response.code()).isEqualTo(0);
        assertThat(response.msg()).isEqualTo("success");
        assertThat(response.expire()).isEqualTo(7200);
        assertThat(response.tenantAccessToken()).isEqualTo("token123");
    }
}
