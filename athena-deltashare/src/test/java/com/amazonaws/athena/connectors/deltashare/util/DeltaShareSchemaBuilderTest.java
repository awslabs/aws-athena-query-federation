/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class DeltaShareSchemaBuilderTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testBuildTableSchemaWithBasicTypes() throws IOException
    {
        String deltaSchemaJson = "{\n" +
            "  \"type\": \"struct\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"long\", \"nullable\": true},\n" +
            "    {\"name\": \"name\", \"type\": \"string\", \"nullable\": false},\n" +
            "    {\"name\": \"age\", \"type\": \"integer\", \"nullable\": true},\n" +
            "    {\"name\": \"active\", \"type\": \"boolean\", \"nullable\": true}\n" +
            "  ]\n" +
            "}";
        
        JsonNode deltaSchema = objectMapper.readTree(deltaSchemaJson);
        Schema schema = DeltaShareSchemaBuilder.buildTableSchema(deltaSchema);
        
        assertNotNull(schema);
        assertEquals(4, schema.getFields().size());
        
        assertNotNull(schema.findField("id"));
        assertNotNull(schema.findField("name"));
        assertNotNull(schema.findField("age"));
        assertNotNull(schema.findField("active"));
        
        assertTrue(schema.findField("id").isNullable());
        assertFalse(schema.findField("name").isNullable());
        assertTrue(schema.findField("age").isNullable());
        assertTrue(schema.findField("active").isNullable());
    }
    
    @Test
    public void testMapDeltaTypeToArrowType()
    {
        ArrowType stringType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("string");
        assertEquals(ArrowType.Utf8.TYPE_TYPE, stringType.getTypeID());
        
        ArrowType longType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("long");
        assertEquals(ArrowType.Int.TYPE_TYPE, longType.getTypeID());
        
        ArrowType intType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("integer");
        assertEquals(ArrowType.Int.TYPE_TYPE, intType.getTypeID());
        
        ArrowType boolType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("boolean");
        assertEquals(ArrowType.Bool.TYPE_TYPE, boolType.getTypeID());
        
        ArrowType doubleType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("double");
        assertEquals(ArrowType.FloatingPoint.TYPE_TYPE, doubleType.getTypeID());
        
        ArrowType floatType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("float");
        assertEquals(ArrowType.FloatingPoint.TYPE_TYPE, floatType.getTypeID());
        
        ArrowType dateType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("date");
        assertEquals(ArrowType.Date.TYPE_TYPE, dateType.getTypeID());
        
        ArrowType timestampType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("timestamp");
        assertEquals(ArrowType.Timestamp.TYPE_TYPE, timestampType.getTypeID());
        
        ArrowType binaryType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("binary");
        assertEquals(ArrowType.Binary.TYPE_TYPE, binaryType.getTypeID());
        
        ArrowType decimalType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("decimal(10,2)");
        assertNotNull(decimalType);
    }
    
    @Test
    public void testMapDeltaTypeToArrowTypeWithUnknownType()
    {
        ArrowType unknownType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("unknown_type");
        assertEquals(ArrowType.Utf8.TYPE_TYPE, unknownType.getTypeID());
        
        try {
            ArrowType nullType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType(null);
            assertEquals(ArrowType.Utf8.TYPE_TYPE, nullType.getTypeID());
        } catch (NullPointerException e) {
            assertTrue(true);
        }
        
        ArrowType emptyType = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("");
        assertEquals(ArrowType.Utf8.TYPE_TYPE, emptyType.getTypeID());
    }
    
    @Test(expected = NullPointerException.class)
    public void testBuildTableSchemaWithNullInput()
    {
        DeltaShareSchemaBuilder.buildTableSchema(null);
    }
    
    @Test
    public void testBuildTableSchemaWithEmptyFields() throws IOException
    {
        String deltaSchemaJson = "{\n" +
            "  \"type\": \"struct\",\n" +
            "  \"fields\": []\n" +
            "}";
        
        JsonNode deltaSchema = objectMapper.readTree(deltaSchemaJson);
        Schema schema = DeltaShareSchemaBuilder.buildTableSchema(deltaSchema);
        
        assertNotNull(schema);
        assertEquals(0, schema.getFields().size());
    }
}
