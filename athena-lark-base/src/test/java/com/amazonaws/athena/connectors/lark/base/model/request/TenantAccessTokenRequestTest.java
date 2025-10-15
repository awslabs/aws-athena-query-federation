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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TenantAccessTokenRequestTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testConstructorAndAccessors() {
        // Arrange
        String appId = "cli_a1b2c3d4e5f6g7h8";
        String appSecret = "secret_abc123xyz789";

        // Act
        TenantAccessTokenRequest request = new TenantAccessTokenRequest(appId, appSecret);

        // Assert
        assertThat(request.appId()).isEqualTo(appId);
        assertThat(request.appSecret()).isEqualTo(appSecret);
    }

    @Test
    void testRecordEquality() {
        // Arrange
        TenantAccessTokenRequest request1 = new TenantAccessTokenRequest("app_123", "secret_456");
        TenantAccessTokenRequest request2 = new TenantAccessTokenRequest("app_123", "secret_456");
        TenantAccessTokenRequest request3 = new TenantAccessTokenRequest("app_999", "secret_000");

        // Assert
        assertThat(request1).isEqualTo(request2);
        assertThat(request1.hashCode()).isEqualTo(request2.hashCode());
        assertThat(request1).isNotEqualTo(request3);
    }

    @Test
    void testRecordToString() {
        // Arrange
        TenantAccessTokenRequest request = new TenantAccessTokenRequest("myAppId", "mySecret");

        // Act
        String toString = request.toString();

        // Assert
        assertThat(toString).contains("TenantAccessTokenRequest");
        assertThat(toString).contains("myAppId");
        assertThat(toString).contains("mySecret");
    }

    @Test
    void testWithNullValues() {
        // Act
        TenantAccessTokenRequest request = new TenantAccessTokenRequest(null, null);

        // Assert
        assertThat(request.appId()).isNull();
        assertThat(request.appSecret()).isNull();
    }

    @Test
    void testJsonSerialization() throws Exception {
        // Arrange
        TenantAccessTokenRequest request = new TenantAccessTokenRequest("test_app", "test_secret");

        // Act
        String json = objectMapper.writeValueAsString(request);

        // Assert
        assertThat(json).contains("\"app_id\"");
        assertThat(json).contains("\"app_secret\"");
        assertThat(json).contains("test_app");
        assertThat(json).contains("test_secret");
    }

    @Test
    void testJsonDeserialization() throws Exception {
        // Arrange
        String json = "{\"app_id\":\"my_app_id\",\"app_secret\":\"my_app_secret\"}";

        // Act
        TenantAccessTokenRequest request = objectMapper.readValue(json, TenantAccessTokenRequest.class);

        // Assert
        assertThat(request.appId()).isEqualTo("my_app_id");
        assertThat(request.appSecret()).isEqualTo("my_app_secret");
    }

    @Test
    void testJsonRoundTrip() throws Exception {
        // Arrange
        TenantAccessTokenRequest original = new TenantAccessTokenRequest("cli_xyz", "secret_123");

        // Act
        String json = objectMapper.writeValueAsString(original);
        TenantAccessTokenRequest deserialized = objectMapper.readValue(json, TenantAccessTokenRequest.class);

        // Assert
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.appId()).isEqualTo(original.appId());
        assertThat(deserialized.appSecret()).isEqualTo(original.appSecret());
    }
}
