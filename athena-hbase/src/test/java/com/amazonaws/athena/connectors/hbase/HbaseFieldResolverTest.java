package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HbaseFieldResolverTest
{

    @Test
    public void getFieldValue()
    {
        String expectedValue = "myValue";
        String family = "family";
        Field field = FieldBuilder.newBuilder("field1", Types.MinorType.VARCHAR.getType()).build();
        Result mockResult = mock(Result.class);
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(family);

        when(mockResult.getValue(any(byte[].class), any(byte[].class))).thenReturn(expectedValue.getBytes());
        Object result = resolver.getFieldValue(field, mockResult);
        assertEquals(expectedValue, result);
    }
}