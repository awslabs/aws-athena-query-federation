/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FieldValueNormalizerTest
{
    @Test
    public void toValueList_nullInput_returnsNull()
    {
        assertNull(FieldValueNormalizer.toValueList(null));
    }

    @Test
    public void toValueList_listInput_returnsSameList()
    {
        List<Object> list = new ArrayList<>();
        list.add("a");
        list.add(1);
        List<Object> result = FieldValueNormalizer.toValueList(list);
        assertNotNull(result);
        assertSame(list, result);
        assertEquals(2, result.size());
        assertEquals("a", result.get(0));
        assertEquals(1, result.get(1));
    }

    @Test
    public void toValueList_emptyListInput_returnsSameList()
    {
        List<Object> list = Collections.emptyList();
        List<Object> result = FieldValueNormalizer.toValueList(list);
        assertNotNull(result);
        assertSame(list, result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void toValueList_stringScalarInput_returnsSingletonList()
    {
        List<Object> result = FieldValueNormalizer.toValueList("hello");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("hello", result.get(0));
    }

    @Test
    public void toValueList_integerScalarInput_returnsSingletonList()
    {
        List<Object> result = FieldValueNormalizer.toValueList(42);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(42, result.get(0));
    }

    @Test
    public void toValueList_longScalarInput_returnsSingletonList()
    {
        List<Object> result = FieldValueNormalizer.toValueList(123L);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(123L, result.get(0));
    }

    @Test
    public void toValueList_booleanScalarInput_returnsSingletonList()
    {
        List<Object> result = FieldValueNormalizer.toValueList(true);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(true, result.get(0));
    }

    @Test
    public void toValueList_emptyMapInput_returnsSingletonListWithEmptyBraces()
    {
        Map<String, Object> map = new HashMap<>();
        List<Object> result = FieldValueNormalizer.toValueList(map);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("{}", result.get(0));
    }

    @Test
    public void toValueList_singleEntryMapInput_returnsSingletonListWithStringRepresentation()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "alice");
        List<Object> result = FieldValueNormalizer.toValueList(map);
        assertNotNull(result);
        assertEquals(1, result.size());
        String str = (String) result.get(0);
        assertTrue(str.startsWith("{"));
        assertTrue(str.endsWith("}"));
        assertTrue(str.contains("name=alice"));
    }

    @Test
    public void toValueList_multipleEntryMapInput_returnsSingletonListWithStringRepresentation()
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "two");
        List<Object> result = FieldValueNormalizer.toValueList(map);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("{a=1,b=two}", result.get(0));
    }

    @Test
    public void toValueList_mapWithNullValue_returnsStringContainingNull()
    {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", null);
        List<Object> result = FieldValueNormalizer.toValueList(map);
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(((String) result.get(0)).contains("null"));
    }

    @Test
    public void toValueList_valueMapStyleListInput_returnsSameList()
    {
        List<Object> valueMapStyle = new ArrayList<>();
        valueMapStyle.add("v1");
        valueMapStyle.add("v2");
        List<Object> result = FieldValueNormalizer.toValueList(valueMapStyle);
        assertNotNull(result);
        assertSame(valueMapStyle, result);
        assertEquals("v1", result.get(0));
        assertEquals("v2", result.get(1));
    }
}
