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

import static org.junit.jupiter.api.Assertions.*;

class NestedUITypeTest
{
    @Test
    void testConstructorAndGetters()
    {
        UITypeEnum uiType = UITypeEnum.TEXT;
        UITypeEnum childType = UITypeEnum.NUMBER;

        NestedUIType nestedUIType = new NestedUIType(uiType, childType);

        assertEquals(uiType, nestedUIType.uiType());
        assertEquals(childType, nestedUIType.childType());
    }

    @Test
    void testEqualsAndHashCode()
    {
        NestedUIType nested1 = new NestedUIType(UITypeEnum.TEXT, UITypeEnum.NUMBER);
        NestedUIType nested2 = new NestedUIType(UITypeEnum.TEXT, UITypeEnum.NUMBER);
        NestedUIType nested3 = new NestedUIType(UITypeEnum.NUMBER, UITypeEnum.TEXT);

        assertEquals(nested1, nested2);
        assertEquals(nested1.hashCode(), nested2.hashCode());
        assertNotEquals(nested1, nested3);
    }

    @Test
    void testToString()
    {
        NestedUIType nestedUIType = new NestedUIType(UITypeEnum.TEXT, UITypeEnum.NUMBER);
        String toString = nestedUIType.toString();

        assertTrue(toString.contains("TEXT"));
        assertTrue(toString.contains("NUMBER"));
    }

    @Test
    void testNullValues()
    {
        NestedUIType nestedUIType = new NestedUIType(null, null);

        assertNull(nestedUIType.uiType());
        assertNull(nestedUIType.childType());
    }

    @Test
    void testDifferentUITypes()
    {
        for (UITypeEnum type : UITypeEnum.values()) {
            NestedUIType nestedUIType = new NestedUIType(type, UITypeEnum.TEXT);
            assertEquals(type, nestedUIType.uiType());
            assertEquals(UITypeEnum.TEXT, nestedUIType.childType());
        }
    }
}
