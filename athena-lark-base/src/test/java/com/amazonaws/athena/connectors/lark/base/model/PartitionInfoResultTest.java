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
package com.amazonaws.athena.connectors.lark.base.model;

import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitionInfoResultTest
{
    @Test
    void testConstructorWithValidMappings()
    {
        String baseId = "base-123";
        String tableId = "table-456";
        NestedUIType nestedType = new NestedUIType(UITypeEnum.TEXT, null);
        List<AthenaFieldLarkBaseMapping> mappings = Arrays.asList(
            new AthenaFieldLarkBaseMapping("field1", "lark_field1", nestedType),
            new AthenaFieldLarkBaseMapping("field2", "lark_field2", nestedType)
        );

        PartitionInfoResult result = new PartitionInfoResult(baseId, tableId, mappings);

        assertEquals(baseId, result.baseId());
        assertEquals(tableId, result.tableId());
        assertEquals(mappings, result.fieldNameMappings());
        assertEquals(2, result.fieldNameMappings().size());
    }

    @Test
    void testConstructorWithNullMappings()
    {
        String baseId = "base-123";
        String tableId = "table-456";

        PartitionInfoResult result = new PartitionInfoResult(baseId, tableId, null);

        assertEquals(baseId, result.baseId());
        assertEquals(tableId, result.tableId());
        assertNotNull(result.fieldNameMappings());
        assertTrue(result.fieldNameMappings().isEmpty());
    }

    @Test
    void testConstructorWithEmptyMappings()
    {
        String baseId = "base-123";
        String tableId = "table-456";
        List<AthenaFieldLarkBaseMapping> mappings = Collections.emptyList();

        PartitionInfoResult result = new PartitionInfoResult(baseId, tableId, mappings);

        assertEquals(baseId, result.baseId());
        assertEquals(tableId, result.tableId());
        assertNotNull(result.fieldNameMappings());
        assertTrue(result.fieldNameMappings().isEmpty());
    }

    @Test
    void testEqualsAndHashCode()
    {
        NestedUIType nestedType = new NestedUIType(UITypeEnum.TEXT, null);
        List<AthenaFieldLarkBaseMapping> mappings = Arrays.asList(
            new AthenaFieldLarkBaseMapping("field1", "lark_field1", nestedType)
        );

        PartitionInfoResult result1 = new PartitionInfoResult("base-1", "table-1", mappings);
        PartitionInfoResult result2 = new PartitionInfoResult("base-1", "table-1", mappings);
        PartitionInfoResult result3 = new PartitionInfoResult("base-2", "table-2", mappings);

        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
        assertNotEquals(result1, result3);
    }

    @Test
    void testToString()
    {
        NestedUIType nestedType = new NestedUIType(UITypeEnum.TEXT, null);
        List<AthenaFieldLarkBaseMapping> mappings = Arrays.asList(
            new AthenaFieldLarkBaseMapping("field1", "lark_field1", nestedType)
        );

        PartitionInfoResult result = new PartitionInfoResult("base-123", "table-456", mappings);
        String toString = result.toString();

        assertTrue(toString.contains("base-123"));
        assertTrue(toString.contains("table-456"));
    }

    @Test
    void testWithNullIds()
    {
        PartitionInfoResult result = new PartitionInfoResult(null, null, null);

        assertNull(result.baseId());
        assertNull(result.tableId());
        assertNotNull(result.fieldNameMappings());
        assertTrue(result.fieldNameMappings().isEmpty());
    }
}
