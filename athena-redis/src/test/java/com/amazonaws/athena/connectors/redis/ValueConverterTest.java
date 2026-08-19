/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ValueConverterTest
{
    private Field mockField(ArrowType arrowType)
    {
        Field field = mock(Field.class);
        when(field.getType()).thenReturn(arrowType);
        when(field.getName()).thenReturn("testField");
        return field;
    }

    @Test
    public void convert_withBigIntType_returnsLongValue()
    {
        Field field = mockField(new ArrowType.Int(64, true));
        Object result = ValueConverter.convert(field, "9223372036854775807");
        assertEquals(9223372036854775807L, result);
    }

    @Test
    public void convert_withFloat4Type_returnsFloatValue()
    {
        Field field = mockField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Object result = ValueConverter.convert(field, "3.14");
        assertEquals(3.14f, (Float) result, 0.0001);
    }

    @Test
    public void convert_withFloat8Type_returnsDoubleValue()
    {
        Field field = mockField(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        Object result = ValueConverter.convert(field, "2.7182818284");
        assertEquals(2.7182818284, (Double) result, 0.0000000001);
    }

    @Test
    public void convert_withBitType_returnsBooleanValue()
    {
        Field field = mockField(new ArrowType.Bool());
        Object result = ValueConverter.convert(field, "true");
        assertEquals(true, result);
    }

    @Test
    public void convert_withVarBinaryType_returnsByteArray()
    {
        Field field = mockField(new ArrowType.Binary());
        Object result = ValueConverter.convert(field, "hello");
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), (byte[]) result);
    }

    @Test
    public void convert_withNullInput_returnsNull()
    {
        Field field = mockField(new ArrowType.Utf8());
        Object result = ValueConverter.convert(field, null);
        assertNull(result);
    }

    @Test(expected = RuntimeException.class)
    public void convert_withUnsupportedType_throwsRuntimeException()
    {
        Field field = mockField(new ArrowType.Decimal(10, 2, 128));
        ValueConverter.convert(field, "100.00");
    }

    @Test(expected = NumberFormatException.class)
    public void convert_withInvalidBigIntFormat_throwsNumberFormatException()
    {
        Field field = mockField(new ArrowType.Int(64, true));
        ValueConverter.convert(field, "not-a-number");
    }

    @Test(expected = NumberFormatException.class)
    public void convert_withInvalidIntegerFormat_throwsNumberFormatException()
    {
        Field field = mockField(new ArrowType.Int(32, true));
        ValueConverter.convert(field, "not-a-number");
    }

    @Test(expected = NumberFormatException.class)
    public void convert_withInvalidFloatFormat_throwsNumberFormatException()
    {
        Field field = mockField(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        ValueConverter.convert(field, "not-a-number");
    }

    @Test(expected = NumberFormatException.class)
    public void convert_withInvalidDoubleFormat_throwsNumberFormatException()
    {
        Field field = mockField(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        ValueConverter.convert(field, "not-a-number");
    }
}
