/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connector.substrait;

import com.amazonaws.athena.connector.substrait.SubstraitTypeAndValue;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SubstraitTypeAndValueTest
{
    @Test
    public void testConstructorWithValidParameters()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        Integer value = 123;
        String columnName = "test_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(type, typeAndValue.getType());
        assertEquals(value, typeAndValue.getValue());
        assertEquals(columnName, typeAndValue.getColumnName());
    }

    @Test
    public void testConstructorWithNullType()
    {
        Integer value = 123;
        String columnName = "test_column";

        assertThrows(NullPointerException.class, () -> {
            new SubstraitTypeAndValue(null, value, columnName);
        });
    }

    @Test
    public void testConstructorWithNullValue()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        String columnName = "test_column";

        assertThrows(NullPointerException.class, () -> {
            new SubstraitTypeAndValue(type, null, columnName);
        });
    }

    @Test
    public void testConstructorWithNullColumnName()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        Integer value = 123;

        assertThrows(NullPointerException.class, () -> {
            new SubstraitTypeAndValue(type, value, null);
        });
    }

    @Test
    public void testGettersWithIntegerType()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        Integer value = 456;
        String columnName = "int_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.INTEGER, typeAndValue.getType());
        assertEquals(Integer.valueOf(456), typeAndValue.getValue());
        assertEquals("int_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithBigIntType()
    {
        SqlTypeName type = SqlTypeName.BIGINT;
        Long value = 9223372036854775807L;
        String columnName = "bigint_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.BIGINT, typeAndValue.getType());
        assertEquals(Long.valueOf(9223372036854775807L), typeAndValue.getValue());
        assertEquals("bigint_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithVarcharType()
    {
        SqlTypeName type = SqlTypeName.VARCHAR;
        String value = "test_string";
        String columnName = "varchar_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.VARCHAR, typeAndValue.getType());
        assertEquals("test_string", typeAndValue.getValue());
        assertEquals("varchar_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithDoubleType()
    {
        SqlTypeName type = SqlTypeName.DOUBLE;
        Double value = 3.141592653589793;
        String columnName = "double_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.DOUBLE, typeAndValue.getType());
        assertEquals(Double.valueOf(3.141592653589793), typeAndValue.getValue());
        assertEquals("double_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithFloatType()
    {
        SqlTypeName type = SqlTypeName.FLOAT;
        Float value = 3.14f;
        String columnName = "float_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.FLOAT, typeAndValue.getType());
        assertEquals(Float.valueOf(3.14f), typeAndValue.getValue());
        assertEquals("float_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithBooleanType()
    {
        SqlTypeName type = SqlTypeName.BOOLEAN;
        Boolean value = true;
        String columnName = "boolean_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.BOOLEAN, typeAndValue.getType());
        assertEquals(Boolean.TRUE, typeAndValue.getValue());
        assertEquals("boolean_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithDecimalType()
    {
        SqlTypeName type = SqlTypeName.DECIMAL;
        BigDecimal value = new BigDecimal("123.45");
        String columnName = "decimal_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.DECIMAL, typeAndValue.getType());
        assertEquals(new BigDecimal("123.45"), typeAndValue.getValue());
        assertEquals("decimal_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithDateType()
    {
        SqlTypeName type = SqlTypeName.DATE;
        String value = "2024-01-01";
        String columnName = "date_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.DATE, typeAndValue.getType());
        assertEquals("2024-01-01", typeAndValue.getValue());
        assertEquals("date_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithTimeType()
    {
        SqlTypeName type = SqlTypeName.TIME;
        String value = "12:30:45";
        String columnName = "time_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.TIME, typeAndValue.getType());
        assertEquals("12:30:45", typeAndValue.getValue());
        assertEquals("time_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithTimestampType()
    {
        SqlTypeName type = SqlTypeName.TIMESTAMP;
        String value = "2024-01-01 12:30:45";
        String columnName = "timestamp_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.TIMESTAMP, typeAndValue.getType());
        assertEquals("2024-01-01 12:30:45", typeAndValue.getValue());
        assertEquals("timestamp_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithVarbinaryType()
    {
        SqlTypeName type = SqlTypeName.VARBINARY;
        byte[] value = "test_bytes".getBytes();
        String columnName = "varbinary_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.VARBINARY, typeAndValue.getType());
        assertEquals(value, typeAndValue.getValue());
        assertEquals("varbinary_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithTinyIntType()
    {
        SqlTypeName type = SqlTypeName.TINYINT;
        Byte value = (byte) 127;
        String columnName = "tinyint_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.TINYINT, typeAndValue.getType());
        assertEquals(Byte.valueOf((byte) 127), typeAndValue.getValue());
        assertEquals("tinyint_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithSmallIntType()
    {
        SqlTypeName type = SqlTypeName.SMALLINT;
        Short value = (short) 32767;
        String columnName = "smallint_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.SMALLINT, typeAndValue.getType());
        assertEquals(Short.valueOf((short) 32767), typeAndValue.getValue());
        assertEquals("smallint_column", typeAndValue.getColumnName());
    }

    @Test
    public void testGettersWithCharType()
    {
        SqlTypeName type = SqlTypeName.CHAR;
        String value = "A";
        String columnName = "char_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);

        assertEquals(SqlTypeName.CHAR, typeAndValue.getType());
        assertEquals("A", typeAndValue.getValue());
        assertEquals("char_column", typeAndValue.getColumnName());
    }

    @Test
    public void testToString()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        Integer value = 123;
        String columnName = "test_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);
        String result = typeAndValue.toString();

        assertTrue(result.contains("TypeAndValue"));
        assertTrue(result.contains("type=" + type));
        assertTrue(result.contains("value=" + value));
    }

    @Test
    public void testToStringWithStringValue()
    {
        SqlTypeName type = SqlTypeName.VARCHAR;
        String value = "hello world";
        String columnName = "varchar_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);
        String result = typeAndValue.toString();

        assertTrue(result.contains("TypeAndValue"));
        assertTrue(result.contains("type=" + type));
        assertTrue(result.contains("value=" + value));
    }

    @Test
    public void testToStringWithComplexValue()
    {
        SqlTypeName type = SqlTypeName.DECIMAL;
        BigDecimal value = new BigDecimal("999.999");
        String columnName = "decimal_column";

        SubstraitTypeAndValue typeAndValue = new SubstraitTypeAndValue(type, value, columnName);
        String result = typeAndValue.toString();

        assertTrue(result.contains("TypeAndValue"));
        assertTrue(result.contains("type=" + type));
        assertTrue(result.contains("value=" + value));
    }

    @Test
    public void testMultipleInstancesWithSameValues()
    {
        SqlTypeName type = SqlTypeName.INTEGER;
        Integer value = 42;
        String columnName = "test_column";

        SubstraitTypeAndValue typeAndValue1 = new SubstraitTypeAndValue(type, value, columnName);
        SubstraitTypeAndValue typeAndValue2 = new SubstraitTypeAndValue(type, value, columnName);

        // Test that both instances have the same values
        assertEquals(typeAndValue1.getType(), typeAndValue2.getType());
        assertEquals(typeAndValue1.getValue(), typeAndValue2.getValue());
        assertEquals(typeAndValue1.getColumnName(), typeAndValue2.getColumnName());
    }

    @Test
    public void testMultipleInstancesWithDifferentValues()
    {
        SubstraitTypeAndValue typeAndValue1 = new SubstraitTypeAndValue(SqlTypeName.INTEGER, 123, "col1");
        SubstraitTypeAndValue typeAndValue2 = new SubstraitTypeAndValue(SqlTypeName.VARCHAR, "test", "col2");

        // Test that instances have different values
        assertEquals(SqlTypeName.INTEGER, typeAndValue1.getType());
        assertEquals(SqlTypeName.VARCHAR, typeAndValue2.getType());
        assertEquals(Integer.valueOf(123), typeAndValue1.getValue());
        assertEquals("test", typeAndValue2.getValue());
        assertEquals("col1", typeAndValue1.getColumnName());
        assertEquals("col2", typeAndValue2.getColumnName());
    }
}
