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
package com.amazonaws.athena.connectors.lark.base.resolver;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link LarkBaseFieldResolver}.
 */
public class LarkBaseFieldResolverTest {

    private LarkBaseFieldResolver resolver;

    @Before
    public void setUp() {
        resolver = new LarkBaseFieldResolver();
    }

    @Test
    public void testGetFieldValueWithNullDataContext() {
        Field field = new Field("testField", FieldType.nullable(new ArrowType.Utf8()), null);

        Object result = resolver.getFieldValue(field, null);

        assertNull("Should return null when dataContext is null", result);
    }

    @Test
    public void testGetFieldValueWithMapDataContextAndExistingKey() {
        Field field = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> dataContext = new HashMap<>();
        dataContext.put("name", "John Doe");
        dataContext.put("age", 30);

        Object result = resolver.getFieldValue(field, dataContext);

        assertEquals("Should return the value for the existing key", "John Doe", result);
    }

    @Test
    public void testGetFieldValueWithMapDataContextAndNonExistingKey() {
        Field field = new Field("email", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> dataContext = new HashMap<>();
        dataContext.put("name", "John Doe");
        dataContext.put("age", 30);

        Object result = resolver.getFieldValue(field, dataContext);

        assertNull("Should return null for non-existing key", result);
    }

    @Test
    public void testGetFieldValueWithMapDataContextAndNullValue() {
        Field field = new Field("description", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> dataContext = new HashMap<>();
        dataContext.put("name", "John Doe");
        dataContext.put("description", null);

        Object result = resolver.getFieldValue(field, dataContext);

        assertNull("Should return null when the value in the map is null", result);
    }

    @Test
    public void testGetFieldValueWithEmptyMap() {
        Field field = new Field("anyField", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> dataContext = new HashMap<>();

        Object result = resolver.getFieldValue(field, dataContext);

        assertNull("Should return null when map is empty", result);
    }

    @Test
    public void testGetFieldValueWithNonMapDataContext() {
        Field field = new Field("someField", FieldType.nullable(new ArrowType.Utf8()), null);

        String dataContext = "I am not a map";

        Object result = resolver.getFieldValue(field, dataContext);

        assertEquals("Should return the dataContext itself when it's not a Map", "I am not a map", result);
    }

    @Test
    public void testGetFieldValueWithNonMapDataContextAsInteger() {
        Field field = new Field("numField", FieldType.nullable(new ArrowType.Int(32, true)), null);

        Integer dataContext = 42;

        Object result = resolver.getFieldValue(field, dataContext);

        assertEquals("Should return the dataContext itself when it's not a Map", 42, result);
    }

    @Test
    public void testGetFieldValueWithMapContainingComplexValues() {
        Field field = new Field("metadata", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("key1", "value1");

        Map<String, Object> dataContext = new HashMap<>();
        dataContext.put("metadata", innerMap);

        Object result = resolver.getFieldValue(field, dataContext);

        assertEquals("Should return the inner map as value", innerMap, result);
    }

    @Test
    public void testGetFieldValueWithFieldNameCaseSensitivity() {
        Field field = new Field("Name", FieldType.nullable(new ArrowType.Utf8()), null);

        Map<String, Object> dataContext = new HashMap<>();
        dataContext.put("name", "lowercase");
        dataContext.put("Name", "uppercase");

        Object result = resolver.getFieldValue(field, dataContext);

        assertEquals("Should be case-sensitive and return exact match", "uppercase", result);
    }
}
