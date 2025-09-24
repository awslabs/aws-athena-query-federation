/*-
 * #%L
 * athena-deltashare
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.deltashare.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertNotEquals;

public class DeltaShareTableTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testConstructorWithValidParameters()
    {
        String tableName = "test_table";
        JsonNode schema = null; 
        
        DeltaShareTable table = new DeltaShareTable(tableName, schema);
        
        assertNotNull("Table should not be null", table);
        assertEquals("Name should match", tableName, table.getName());
        assertEquals("Schema should match", schema, table.getSchema());
    }
    
    @Test
    public void testConstructorWithSchema() throws IOException
    {
        String tableName = "products";
        String schemaJson = "{\n" +
            "  \"type\": \"struct\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"long\", \"nullable\": true},\n" +
            "    {\"name\": \"name\", \"type\": \"string\", \"nullable\": false}\n" +
            "  ]\n" +
            "}";
        JsonNode schema = objectMapper.readTree(schemaJson);
        
        DeltaShareTable table = new DeltaShareTable(tableName, schema);
        
        assertNotNull("Table should not be null", table);
        assertEquals("Name should match", tableName, table.getName());
        assertEquals("Schema should match", schema, table.getSchema());
        assertNotNull("Schema should not be null", table.getSchema());
        assertTrue("Schema should have fields", table.getSchema().has("fields"));
    }
    
    @Test
    public void testConstructorWithNullName()
    {
        String tableName = null;
        JsonNode schema = null;
        
        DeltaShareTable table = new DeltaShareTable(tableName, schema);
        
        assertNotNull("Table should not be null", table);
        assertNull("Name should be null", table.getName());
        assertNull("Schema should be null", table.getSchema());
    }
    
    @Test
    public void testConstructorWithEmptyName()
    {
        String tableName = "";
        JsonNode schema = null;
        
        DeltaShareTable table = new DeltaShareTable(tableName, schema);
        
        assertNotNull("Table should not be null", table);
        assertEquals("Name should be empty string", "", table.getName());
        assertNull("Schema should be null", table.getSchema());
    }
    
    @Test
    public void testGetName()
    {
        String expectedName = "customer_data";
        DeltaShareTable table = new DeltaShareTable(expectedName, null);
        
        assertEquals("getName should return the name", expectedName, table.getName());
    }
    
    @Test
    public void testGetNameWithSpecialCharacters()
    {
        String specialName = "table_with-special.chars123";
        DeltaShareTable table = new DeltaShareTable(specialName, null);
        
        assertEquals("getName should handle special characters", specialName, table.getName());
    }
    
    @Test
    public void testGetSchema()
    {
        JsonNode expectedSchema = objectMapper.createObjectNode();
        DeltaShareTable table = new DeltaShareTable("test", expectedSchema);
        
        assertEquals("getSchema should return the schema", expectedSchema, table.getSchema());
    }
    
    @Test
    public void testGetSchemaWithComplexSchema() throws IOException
    {
        String complexSchemaJson = "{\n" +
            "  \"type\": \"struct\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"nested\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"struct\",\n" +
            "        \"fields\": [\n" +
            "          {\"name\": \"inner_id\", \"type\": \"long\", \"nullable\": true}\n" +
            "        ]\n" +
            "      },\n" +
            "      \"nullable\": true\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"tags\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"elementType\": \"string\",\n" +
            "        \"containsNull\": true\n" +
            "      },\n" +
            "      \"nullable\": true\n" +
            "    }\n" +
            "  ]\n" +
            "}";
        JsonNode schema = objectMapper.readTree(complexSchemaJson);
        
        DeltaShareTable table = new DeltaShareTable("complex_table", schema);
        
        assertNotNull("Table should not be null", table);
        assertEquals("Schema should match", schema, table.getSchema());
        assertTrue("Schema should have type field", table.getSchema().has("type"));
        assertTrue("Schema should have fields", table.getSchema().has("fields"));
        assertEquals("Should have 2 fields", 2, table.getSchema().get("fields").size());
    }
    
    @Test
    public void testMultipleInstances() throws IOException
    {
        
        String schema1Json = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true}]}";
        String schema2Json = "{\"type\":\"struct\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":false}]}";
        
        JsonNode schema1 = objectMapper.readTree(schema1Json);
        JsonNode schema2 = objectMapper.readTree(schema2Json);
        
        DeltaShareTable table1 = new DeltaShareTable("table1", schema1);
        DeltaShareTable table2 = new DeltaShareTable("table2", schema2);
        DeltaShareTable table3 = new DeltaShareTable("table3", null);
        
        
        assertEquals("Table1 name", "table1", table1.getName());
        assertEquals("Table2 name", "table2", table2.getName());
        assertEquals("Table3 name", "table3", table3.getName());
        
        assertEquals("Table1 schema", schema1, table1.getSchema());
        assertEquals("Table2 schema", schema2, table2.getSchema());
        assertNull("Table3 schema should be null", table3.getSchema());
        
        
        assertNotEquals("Tables should have different names", table1.getName(), table2.getName());
        assertNotEquals("Tables should have different schemas", table1.getSchema(), table2.getSchema());
    }
    
    @Test
    public void testTableWithLongName()
    {
        String longName = "very_long_table_name_with_lots_of_characters_that_might_be_used_in_real_world_scenarios_like_data_lake_tables";
        DeltaShareTable table = new DeltaShareTable(longName, null);
        
        assertEquals("Should handle long names", longName, table.getName());
    }
    
    @Test
    public void testTableWithUnicodeCharacters()
    {
        String unicodeName = "table_with_unicode_测试_表";
        DeltaShareTable table = new DeltaShareTable(unicodeName, null);
        
        assertEquals("Should handle unicode characters", unicodeName, table.getName());
    }
    
    @Test
    public void testTableImmutability()
    {
        String originalName = "immutable_table";
        JsonNode originalSchema = objectMapper.createObjectNode();
        
        DeltaShareTable table = new DeltaShareTable(originalName, originalSchema);
        
        
        assertSame("Name should be same reference", originalName, table.getName());
        assertSame("Schema should be same reference", originalSchema, table.getSchema());
        
        
        assertEquals("Name should remain unchanged", originalName, table.getName());
        assertEquals("Schema should remain unchanged", originalSchema, table.getSchema());
    }
    
    
    
}
