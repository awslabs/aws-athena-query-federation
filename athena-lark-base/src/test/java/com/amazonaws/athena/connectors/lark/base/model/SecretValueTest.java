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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SecretValueTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testConstructorAndGetters()
    {
        String appId = "test-app-id";
        String appSecret = "test-app-secret";

        SecretValue secretValue = new SecretValue(appId, appSecret);

        assertEquals(appId, secretValue.larkAppId());
        assertEquals(appSecret, secretValue.larkAppSecret());
    }

    @Test
    void testEqualsAndHashCode()
    {
        SecretValue secretValue1 = new SecretValue("app-id-1", "app-secret-1");
        SecretValue secretValue2 = new SecretValue("app-id-1", "app-secret-1");
        SecretValue secretValue3 = new SecretValue("app-id-2", "app-secret-2");

        assertEquals(secretValue1, secretValue2);
        assertEquals(secretValue1.hashCode(), secretValue2.hashCode());
        assertNotEquals(secretValue1, secretValue3);
    }

    @Test
    void testToString()
    {
        SecretValue secretValue = new SecretValue("app-id", "app-secret");
        String toString = secretValue.toString();

        assertTrue(toString.contains("app-id"));
        assertTrue(toString.contains("app-secret"));
    }

    @Test
    void testJsonSerialization() throws Exception
    {
        SecretValue secretValue = new SecretValue("test-app-id", "test-app-secret");

        String json = objectMapper.writeValueAsString(secretValue);

        assertTrue(json.contains("lark_app_id"));
        assertTrue(json.contains("test-app-id"));
        assertTrue(json.contains("lark_app_secret"));
        assertTrue(json.contains("test-app-secret"));
    }

    @Test
    void testJsonDeserialization() throws Exception
    {
        String json = "{\"lark_app_id\":\"test-app-id\",\"lark_app_secret\":\"test-app-secret\"}";

        SecretValue secretValue = objectMapper.readValue(json, SecretValue.class);

        assertEquals("test-app-id", secretValue.larkAppId());
        assertEquals("test-app-secret", secretValue.larkAppSecret());
    }

    @Test
    void testNullValues()
    {
        SecretValue secretValue = new SecretValue(null, null);

        assertNull(secretValue.larkAppId());
        assertNull(secretValue.larkAppSecret());
    }

    @Test
    void testEmptyValues()
    {
        SecretValue secretValue = new SecretValue("", "");

        assertEquals("", secretValue.larkAppId());
        assertEquals("", secretValue.larkAppSecret());
    }
}
