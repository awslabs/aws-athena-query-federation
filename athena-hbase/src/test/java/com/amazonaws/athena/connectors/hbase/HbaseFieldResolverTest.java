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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.nullable;
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
        HbaseFieldResolver resolver = HbaseFieldResolver.resolver(false, family);

        when(mockResult.getValue(nullable(byte[].class), nullable(byte[].class))).thenReturn(expectedValue.getBytes());
        Object result = resolver.getFieldValue(field, mockResult);
        assertEquals(expectedValue, result);
    }
}
