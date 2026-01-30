/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.BsonTimestamp;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeUtilsTest
{
    private static final String FLOAT8_FIELD = "float8";
    private static final String FLOAT4_FIELD = "float4";
    private static final String INT_FIELD = "int";
    private static final String DATE_FIELD = "date";
    private static final int INT_VALUE_42 = 42;
    private static final double DOUBLE_VALUE_42_0 = 42.0;
    private static final double DOUBLE_VALUE_42_5 = 42.5;
    private static final float FLOAT_VALUE_42_0 = 42.0f;
    private static final float FLOAT_VALUE_42_5 = 42.5f;
    private static final float FLOAT_VALUE_42_7 = 42.7f;
    private static final double DOUBLE_VALUE_42_7 = 42.7d;
    private static final int TIMESTAMP_SECONDS = 1000;
    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final long EXPECTED_TIMESTAMP_MILLIS = TIMESTAMP_SECONDS * MILLISECONDS_PER_SECOND;
    private static final double DELTA = 0.001;
    private static final float FLOAT_DELTA = 0.001f;

    @Test
    public void unsupportedCoerce()
    {
        Object result = TypeUtils.coerce(FieldBuilder.newBuilder("unsupported", Types.MinorType.VARCHAR.getType()).build(), new UnsupportedType());
        assertEquals("UnsupportedType{}", result);
        assertTrue(result instanceof String);
    }

    @Test
    public void coerce_withFloat8Field_returnsDoubleValue()
    {
        Field float8Field = FieldBuilder.newBuilder(FLOAT8_FIELD, Types.MinorType.FLOAT8.getType()).build();
        
        // Test Integer to Double conversion
        Object intResult = TypeUtils.coerce(float8Field, INT_VALUE_42);
        assertTrue("Result should be Double", intResult instanceof Double);
        assertEquals(DOUBLE_VALUE_42_0, (Double) intResult, DELTA);

        // Test Float to Double conversion
        Object floatResult = TypeUtils.coerce(float8Field, FLOAT_VALUE_42_5);
        assertTrue("Result should be Double", floatResult instanceof Double);
        assertEquals(DOUBLE_VALUE_42_5, (Double) floatResult, DELTA);

        // Test passing Double directly
        Object doubleResult = TypeUtils.coerce(float8Field, DOUBLE_VALUE_42_5);
        assertTrue("Result should be Double", doubleResult instanceof Double);
        assertEquals(DOUBLE_VALUE_42_5, (Double) doubleResult, DELTA);
    }

    @Test
    public void coerce_withFloat4Field_returnsFloatValue()
    {
        Field float4Field = FieldBuilder.newBuilder(FLOAT4_FIELD, Types.MinorType.FLOAT4.getType()).build();
        
        // Test Integer to Float conversion
        Object intResult = TypeUtils.coerce(float4Field, INT_VALUE_42);
        assertTrue("Result should be Float", intResult instanceof Float);
        assertEquals(FLOAT_VALUE_42_0, (Float) intResult, FLOAT_DELTA);

        // Test Double to Float conversion
        Object doubleResult = TypeUtils.coerce(float4Field, DOUBLE_VALUE_42_5);
        assertTrue("Result should be Float", doubleResult instanceof Float);
        assertEquals(FLOAT_VALUE_42_5, (Float) doubleResult, FLOAT_DELTA);

        // Test passing Float directly
        Object floatResult = TypeUtils.coerce(float4Field, FLOAT_VALUE_42_5);
        assertTrue("Result should be Float", floatResult instanceof Float);
        assertEquals(FLOAT_VALUE_42_5, (Float) floatResult, FLOAT_DELTA);
    }

    @Test
    public void coerce_withIntField_returnsIntegerValue()
    {
        Field intField = FieldBuilder.newBuilder(INT_FIELD, Types.MinorType.INT.getType()).build();
        
        // Test Float to Int conversion - truncates decimal part
        Object floatResult = TypeUtils.coerce(intField, FLOAT_VALUE_42_7);
        assertTrue("Result should be Integer", floatResult instanceof Integer);
        assertEquals(INT_VALUE_42, floatResult);

        // Test Double to Int conversion - truncates decimal part
        Object doubleResult = TypeUtils.coerce(intField, DOUBLE_VALUE_42_7);
        assertTrue("Result should be Integer", doubleResult instanceof Integer);
        assertEquals(INT_VALUE_42, doubleResult);

        // Test passing Integer directly
        Object intResult = TypeUtils.coerce(intField, INT_VALUE_42);
        assertTrue("Result should be Integer", intResult instanceof Integer);
        assertEquals(INT_VALUE_42, intResult);
    }

    @Test
    public void coerce_withDateMilliField_returnsDateValue()
    {
        Field dateField = FieldBuilder.newBuilder(DATE_FIELD, Types.MinorType.DATEMILLI.getType()).build();
        
        // Create a BsonTimestamp with a known time value
        BsonTimestamp bsonTimestamp = new BsonTimestamp(TIMESTAMP_SECONDS, 0);
        
        // Test BsonTimestamp to Date conversion
        Object result = TypeUtils.coerce(dateField, bsonTimestamp);
        assertTrue("Result should be Date", result instanceof Date);
        assertEquals(EXPECTED_TIMESTAMP_MILLIS, ((Date) result).getTime());

        // Test passing Date directly
        Date date = new Date(EXPECTED_TIMESTAMP_MILLIS);
        Object dateResult = TypeUtils.coerce(dateField, date);
        assertTrue("Result should be Date", dateResult instanceof Date);
        assertEquals(date, dateResult);
    }
}
