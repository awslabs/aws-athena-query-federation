/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HbaseFieldResolverTest
{
    private static final String FAMILY = "family";
    private static final String FIELD_NAME = "field1";
    private static final String EXPECTED_VALUE = "myValue";

    @Test
    public void getFieldValue_withValidResult_returnsResolvedFieldValue()
    {
        Field field = FieldBuilder.newBuilder(FIELD_NAME, Types.MinorType.VARCHAR.getType()).build();
        Result mockResult = mock(Result.class);
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(false, FAMILY);

        when(mockResult.getValue(nullable(byte[].class), nullable(byte[].class))).thenReturn(EXPECTED_VALUE.getBytes());
        Object result = resolver.getFieldValue(field, mockResult);
        assertEquals("Field value should match expected", EXPECTED_VALUE, result);
    }

    @Test
    public void getFieldValue_withNullValue_returnsNull()
    {
        Field field = FieldBuilder.newBuilder(FIELD_NAME, Types.MinorType.VARCHAR.getType()).build();
        Result mockResult = mock(Result.class);
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(false, FAMILY);

        when(mockResult.getValue(nullable(byte[].class), nullable(byte[].class))).thenReturn(null);
        Object result = resolver.getFieldValue(field, mockResult);
        assertNull("Field value should be null", result);
    }

    @Test
    public void getFieldValue_withNativeStorage_returnsCoercedValue()
    {
        Field field = FieldBuilder.newBuilder(FIELD_NAME, Types.MinorType.BIGINT.getType()).build();
        Result mockResult = mock(Result.class);
        HbaseFieldResolver resolver = new HbaseFieldResolver(true, FAMILY.getBytes());

        byte[] nativeValue = new byte[8];
        java.nio.ByteBuffer.wrap(nativeValue).putLong(12345L);
        when(mockResult.getValue(nullable(byte[].class), nullable(byte[].class))).thenReturn(nativeValue);
        Object result = resolver.getFieldValue(field, mockResult);
        assertNotNull("Field value should not be null", result);
        assertEquals("Field value should be coerced to Long", 12345L, result);
    }

    @Test
    public void getFieldValue_withInvalidValueType_throwsIllegalArgumentException()
    {
        assertGetFieldValueThrowsIllegalArgumentException("not a Result", "Expected value of type Result");
    }

    @Test
    public void getFieldValue_withNullValueType_throwsIllegalArgumentException()
    {
        assertGetFieldValueThrowsIllegalArgumentException(null, "null");
    }

    @Test
    public void resolver_withValidFamily_createsResolver()
    {
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(false, FAMILY);
        assertNotNull("Resolver should not be null", resolver);
    }

    private void assertGetFieldValueThrowsIllegalArgumentException(Object invalidValue, String expectedMessageSubstring)
    {
        Field field = FieldBuilder.newBuilder(FIELD_NAME, Types.MinorType.VARCHAR.getType()).build();
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(false, FAMILY);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                resolver.getFieldValue(field, invalidValue));
        assertTrue("Exception message should contain " + expectedMessageSubstring,
                ex.getMessage() != null && ex.getMessage().contains(expectedMessageSubstring));
    }
}
