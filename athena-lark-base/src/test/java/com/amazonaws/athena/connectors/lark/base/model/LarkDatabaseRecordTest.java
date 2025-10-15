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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LarkDatabaseRecordTest
{
    @Test
    void testConstructorAndGetters()
    {
        String id = "db-123";
        String name = "Test Database";

        LarkDatabaseRecord record = new LarkDatabaseRecord(id, name);

        assertEquals(id, record.id());
        assertEquals(name, record.name());
    }

    @Test
    void testEqualsAndHashCode()
    {
        LarkDatabaseRecord record1 = new LarkDatabaseRecord("db-1", "Database 1");
        LarkDatabaseRecord record2 = new LarkDatabaseRecord("db-1", "Database 1");
        LarkDatabaseRecord record3 = new LarkDatabaseRecord("db-2", "Database 2");

        assertEquals(record1, record2);
        assertEquals(record1.hashCode(), record2.hashCode());
        assertNotEquals(record1, record3);
    }

    @Test
    void testToString()
    {
        LarkDatabaseRecord record = new LarkDatabaseRecord("db-123", "Test DB");
        String toString = record.toString();

        assertTrue(toString.contains("db-123"));
        assertTrue(toString.contains("Test DB"));
    }

    @Test
    void testNullValues()
    {
        LarkDatabaseRecord record = new LarkDatabaseRecord(null, null);

        assertNull(record.id());
        assertNull(record.name());
    }

    @Test
    void testEmptyValues()
    {
        LarkDatabaseRecord record = new LarkDatabaseRecord("", "");

        assertEquals("", record.id());
        assertEquals("", record.name());
    }
}
