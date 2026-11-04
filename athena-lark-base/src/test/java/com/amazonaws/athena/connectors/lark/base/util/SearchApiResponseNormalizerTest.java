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
package com.amazonaws.athena.connectors.lark.base.util;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class SearchApiResponseNormalizerTest {

    @Test
    public void testNormalizeRecordFields_nullInput_returnsEmptyMap() {
        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(null);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testNormalizeRecordFields_emptyInput_returnsEmptyMap() {
        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(new HashMap<>());
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testNormalizeFieldValue_textField_extractsTextFromArray() {
        // Search API: [{"text": "Sample text", "type": "text"}]
        // Expected: "Sample text"
        Map<String, Object> textObject = new HashMap<>();
        textObject.put("text", "Sample text");
        textObject.put("type", "text");
        List<Map<String, Object>> searchApiValue = Collections.singletonList(textObject);

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_text", searchApiValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertEquals("Sample text", result.get("field_text"));
    }

    @Test
    public void testNormalizeFieldValue_textField_multipleElements_keepsArray() {
        // Multiple text elements - should keep as array
        Map<String, Object> text1 = Map.of("text", "Text 1", "type", "text");
        Map<String, Object> text2 = Map.of("text", "Text 2", "type", "text");
        List<Map<String, Object>> searchApiValue = Arrays.asList(text1, text2);

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_text", searchApiValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertTrue(result.get("field_text") instanceof List);
    }

    @Test
    public void testNormalizeFieldValue_formulaField_unwrapsValue() {
        // Search API: {"type": 1, "value": [{"text": "Calculated", "type": "text"}]}
        // Expected: "Calculated" (unwraps then extracts text from single-element array)
        Map<String, Object> innerValue = Map.of("text", "Calculated", "type", "text");
        Map<String, Object> wrappedValue = new HashMap<>();
        wrappedValue.put("type", 1);
        wrappedValue.put("value", Collections.singletonList(innerValue));

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_formula", wrappedValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        // Recursive normalization: unwraps formula, then extracts text from single-element array
        assertEquals("Calculated", result.get("field_formula"));
    }

    @Test
    public void testNormalizeFieldValue_createdUser_extractsFirstElement() {
        // Search API: [{"id": "user123", "name": "John Doe"}]
        // Expected: {"id": "user123", "name": "John Doe"}
        Map<String, Object> userObject = new HashMap<>();
        userObject.put("id", "user123");
        userObject.put("name", "John Doe");
        List<Map<String, Object>> searchApiValue = Collections.singletonList(userObject);

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_created_user", searchApiValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertTrue(result.get("field_created_user") instanceof Map);
        Map<?, ?> user = (Map<?, ?>) result.get("field_created_user");
        assertEquals("user123", user.get("id"));
        assertEquals("John Doe", user.get("name"));
    }

    @Test
    public void testNormalizeFieldValue_modifiedUser_extractsFirstElement() {
        // Search API: [{"id": "user456", "name": "Jane Doe"}]
        // Expected: {"id": "user456", "name": "Jane Doe"}
        Map<String, Object> userObject = new HashMap<>();
        userObject.put("id", "user456");
        userObject.put("name", "Jane Doe");
        List<Map<String, Object>> searchApiValue = Collections.singletonList(userObject);

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_modified_user", searchApiValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertTrue(result.get("field_modified_user") instanceof Map);
        Map<?, ?> user = (Map<?, ?>) result.get("field_modified_user");
        assertEquals("user456", user.get("id"));
    }

    @Test
    public void testNormalizeFieldValue_regularUserField_keepsArray() {
        // Regular user field (not created_user/modified_user) should keep array
        Map<String, Object> userObject = Map.of("id", "user789", "name", "Bob Smith");
        List<Map<String, Object>> searchApiValue = Collections.singletonList(userObject);

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_user", searchApiValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertTrue(result.get("field_user") instanceof List);
    }

    @Test
    public void testNormalizeFieldValue_numberField_passesThrough() {
        // Number fields already come as numbers from Search API
        Map<String, Object> fields = new HashMap<>();
        fields.put("field_number", 123.456);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertEquals(123.456, result.get("field_number"));
    }

    @Test
    public void testNormalizeFieldValue_nullValue_returnsNull() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("field_null", null);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertNull(result.get("field_null"));
    }

    @Test
    public void testNormalizeFieldValue_emptyArray_returnsEmptyArray() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("field_empty", Collections.emptyList());

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertTrue(result.get("field_empty") instanceof List);
        assertTrue(((List<?>) result.get("field_empty")).isEmpty());
    }

    @Test
    public void testNormalizeRecordFields_multipleFieldTypes() {
        // Test multiple field types together
        Map<String, Object> fields = new HashMap<>();

        // Text field
        fields.put("field_text", Collections.singletonList(
            Map.of("text", "Sample", "type", "text")));

        // Number field
        fields.put("field_number", 42.0);

        // Created user
        fields.put("field_created_user", Collections.singletonList(
            Map.of("id", "u1", "name", "User One")));

        // Formula wrapped value
        Map<String, Object> formulaValue = new HashMap<>();
        formulaValue.put("type", 1);
        formulaValue.put("value", 100);
        fields.put("field_formula", formulaValue);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        assertEquals(4, result.size());
        assertEquals("Sample", result.get("field_text"));
        assertEquals(42.0, result.get("field_number"));
        assertTrue(result.get("field_created_user") instanceof Map);
        assertEquals(100, result.get("field_formula"));
    }

    @Test
    public void testNormalizeFieldValue_nestedFormulaWithTextArray() {
        // Complex case: Formula wrapping text array
        Map<String, Object> innerText = Map.of("text", "Nested Text", "type", "text");
        Map<String, Object> formulaWrapper = new HashMap<>();
        formulaWrapper.put("type", 1);
        formulaWrapper.put("value", Collections.singletonList(innerText));

        Map<String, Object> fields = new HashMap<>();
        fields.put("field_lookup", formulaWrapper);

        Map<String, Object> result = SearchApiResponseNormalizer.normalizeRecordFields(fields);

        // Should unwrap formula, then extract text from single-element array
        assertEquals("Nested Text", result.get("field_lookup"));
    }
}
