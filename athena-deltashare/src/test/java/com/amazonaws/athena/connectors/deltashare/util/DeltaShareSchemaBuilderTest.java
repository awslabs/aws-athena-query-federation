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
package com.amazonaws.athena.connectors.deltashare.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DeltaShareSchemaBuilderTest
{
    @Rule
    public TestName testName = new TestName();

    @Test
    public void testMapDeltaTypeToArrowTypeString()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("string");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeInteger()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("integer");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
        assertTrue(((ArrowType.Int) result).getIsSigned());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeInt()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("int");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(32, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeLong()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("long");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) result).getBitWidth());
        assertTrue(((ArrowType.Int) result).getIsSigned());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeBigInt()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("bigint");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Int);
        assertEquals(64, ((ArrowType.Int) result).getBitWidth());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeDouble()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("double");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeFloat()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("float");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.FloatingPoint);
        assertEquals(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE, ((ArrowType.FloatingPoint) result).getPrecision());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeBoolean()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("boolean");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Bool);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeDate()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("date");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Date);
        assertEquals(org.apache.arrow.vector.types.DateUnit.DAY, ((ArrowType.Date) result).getUnit());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeTimestamp()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("timestamp");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Timestamp);
        assertEquals(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, ((ArrowType.Timestamp) result).getUnit());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeBinary()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("binary");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Binary);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeDecimal()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("decimal");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Decimal);
        assertEquals(18, ((ArrowType.Decimal) result).getPrecision());
        assertEquals(2, ((ArrowType.Decimal) result).getScale());
    }

    @Test
    public void testMapDeltaTypeToArrowTypeDecimalWithDefaultScale()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("decimal(18)");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeArray()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("array<string>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeMap()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("map<string,integer>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeStruct()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("struct<field1:string,field2:integer>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeUnknown()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("unknown_type");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeCaseInsensitive()
    {
        ArrowType result1 = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("STRING");
        ArrowType result2 = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("String");
        ArrowType result3 = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("string");

        assertTrue(result1 instanceof ArrowType.Utf8);
        assertTrue(result2 instanceof ArrowType.Utf8);
        assertTrue(result3 instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeWithWhitespace()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("  string  ");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeComplexDecimal()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("decimal(38,18)");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeNestedArray()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("array<array<string>>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeComplexStruct()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("struct<name:string,age:integer,address:struct<street:string,city:string>>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeComplexMap()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("map<string,struct<field1:string,field2:integer>>");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test(expected = NullPointerException.class)
    public void testMapDeltaTypeToArrowTypeNullInput()
    {
        DeltaShareSchemaBuilder.mapDeltaTypeToArrowType(null);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeEmptyInput()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeVarchar()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("varchar(255)");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeChar()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("char(10)");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeTinyInt()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("tinyint");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeSmallInt()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("smallint");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeReal()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("real");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }

    @Test
    public void testMapDeltaTypeToArrowTypeInterval()
    {
        ArrowType result = DeltaShareSchemaBuilder.mapDeltaTypeToArrowType("interval");
        assertNotNull(result);
        assertTrue(result instanceof ArrowType.Utf8);
    }
}
