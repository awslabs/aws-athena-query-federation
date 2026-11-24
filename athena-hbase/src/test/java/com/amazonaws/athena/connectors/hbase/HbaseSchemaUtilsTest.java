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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.ResultProcessor;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.hbase.HbaseSchemaUtils.coerceType;
import static com.amazonaws.athena.connectors.hbase.HbaseSchemaUtils.toBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseSchemaUtilsTest
{
    @Test
    public void inferSchema_withClientAndTable_returnsSchema()
            throws IOException
    {
        int numToScan = 4;
        TableName tableName = new TableName("schema", "table");
        List<Result> results = TestUtils.makeResults();

        HBaseConnection mockConnection = mock(HBaseConnection.class);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());
        when(mockConnection.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });
        when(mockConnection.tableExists(any())).thenReturn(true);

        Schema schema = HbaseSchemaUtils.inferSchema(mockConnection, HbaseTableNameUtils.getQualifiedTable(tableName), numToScan);

        Map<String, Types.MinorType> actualFields = new HashMap<>();
        schema.getFields().stream().forEach(next -> actualFields.put(next.getName(), Types.getMinorTypeForArrowType(next.getType())));

        Map<String, Types.MinorType> expectedFields = new HashMap<>();
        TestUtils.makeSchema().build().getFields().stream()
                .forEach(next -> expectedFields.put(next.getName(), Types.getMinorTypeForArrowType(next.getType())));

        for (Map.Entry<String, Types.MinorType> nextExpected : expectedFields.entrySet()) {
            assertNotNull("Field " + nextExpected.getKey() + " should be present", actualFields.get(nextExpected.getKey()));
            assertEquals("Field type should match for " + nextExpected.getKey(), nextExpected.getValue(), actualFields.get(nextExpected.getKey()));
        }
        assertEquals("Schema field count should match", expectedFields.size(), actualFields.size());

        verify(mockConnection, times(1)).scanTable(any(), nullable(Scan.class), nullable(ResultProcessor.class));
        verify(mockScanner, times(1)).iterator();
    }

    @Test
    public void inferType_withVariousStrings_returnsExpectedMinorTypes()
    {
        assertEquals("Integer string should infer BIGINT", Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("1"));
        assertEquals("Integer string should infer BIGINT", Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("1000"));
        assertEquals("Negative integer should infer BIGINT", Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("-1"));
        assertEquals("Decimal string should infer FLOAT8", Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType("1.0"));
        assertEquals("Decimal string should infer FLOAT8", Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType(".01"));
        assertEquals("Negative decimal should infer FLOAT8", Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType("-.01"));
        assertEquals("Non-numeric string should infer VARCHAR", Types.MinorType.VARCHAR, HbaseSchemaUtils.inferType("BDFKD"));
        assertEquals("Empty string should infer VARCHAR", Types.MinorType.VARCHAR, HbaseSchemaUtils.inferType(""));
    }

    @Test
    public void coerceType_withStringStorage_returnsCoercedValues()
    {
        boolean isNative = false;
        assertEquals("asf", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "asf".getBytes()));
        assertEquals("2.0", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "2.0".getBytes()));
        assertEquals("Coerced INT from string 1 should be 1", 1, coerceType(isNative, Types.MinorType.INT.getType(), "1".getBytes()));
        assertEquals("Coerced INT from string -1 should be -1", -1, coerceType(isNative, Types.MinorType.INT.getType(), "-1".getBytes()));
        assertEquals(1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), "1".getBytes()));
        assertEquals(-1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), "-1".getBytes()));
        assertEquals(1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), "1.1".getBytes()));
        assertEquals(-1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), "-1.1".getBytes()));
        assertEquals(1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), "1.1".getBytes()));
        assertEquals(-1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), "-1.1".getBytes()));
        assertArrayEquals("-1.1".getBytes(), (byte[]) coerceType(isNative, Types.MinorType.VARBINARY.getType(), "-1.1".getBytes()));
    }

    @Test
    public void coerceType_withNativeStorage_returnsCoercedValues()
    {
        boolean isNative = true;
        assertEquals("asf", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "asf".getBytes()));
        assertEquals("2.0", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "2.0".getBytes()));
        assertEquals("Coerced INT from native bytes 1 should be 1", 1, coerceType(isNative, Types.MinorType.INT.getType(), toBytes(isNative, 1)));
        assertEquals("Coerced INT from native bytes -1 should be -1", -1, coerceType(isNative, Types.MinorType.INT.getType(), toBytes(isNative, -1)));
        assertEquals(1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), toBytes(isNative, 1L)));
        assertEquals(-1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), toBytes(isNative, -1L)));
        assertEquals(1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), toBytes(isNative, 1.1F)));
        assertEquals(-1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), toBytes(isNative, -1.1F)));
        assertEquals(1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), toBytes(isNative, 1.1D)));
        assertEquals(-1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), toBytes(isNative, -1.1D)));
        assertArrayEquals("-1.1".getBytes(), (byte[]) coerceType(isNative, Types.MinorType.VARBINARY.getType(), "-1.1".getBytes()));
    }

    @Test
    public void extractColumnParts_withFamilyColumnFormat_returnsParts()
    {
        String[] parts = HbaseSchemaUtils.extractColumnParts("family:column");
        assertEquals("First part should be family", "family", parts[0]);
        assertEquals("Second part should be column", "column", parts[1]);
    }

    @Test
    public void coerceType_withNullValue_returnsNull()
    {
        // Test that null input is handled correctly without throwing exceptions
        Object result = HbaseSchemaUtils.coerceType(false, Types.MinorType.VARCHAR.getType(), null);
        assertNull("Result should be null when value is null", result);
        
        // Verify it works with different types as well
        Object result2 = HbaseSchemaUtils.coerceType(true, Types.MinorType.BIGINT.getType(), null);
        assertNull("Result should be null when value is null for native type", result2);
    }

    @Test
    public void coerceType_withNativeBit_returnsBoolean()
    {
        byte[] trueValue = new byte[] {1};
        byte[] falseValue = new byte[] {0};
        assertEquals("True value should return true", true, coerceType(true, Types.MinorType.BIT.getType(), trueValue));
        assertEquals("False value should return false", false, coerceType(true, Types.MinorType.BIT.getType(), falseValue));
    }

    @Test
    public void coerceType_withStringBit_returnsBoolean()
    {
        assertEquals("True string should return true", true, coerceType(false, Types.MinorType.BIT.getType(), "true".getBytes()));
        assertEquals("False string should return false", false, coerceType(false, Types.MinorType.BIT.getType(), "false".getBytes()));
    }

    @Test
    public void toBytes_withNullValue_returnsNull()
    {
        // Test that null input is handled correctly without throwing exceptions
        byte[] result = HbaseSchemaUtils.toBytes(false, null);
        assertNull("Result should be null when value is null", result);
        
        // Verify it works with native storage as well
        byte[] result2 = HbaseSchemaUtils.toBytes(true, null);
        assertNull("Result should be null when value is null for native storage", result2);
    }

    @Test
    public void toBytes_withByteArray_returnsSame()
    {
        byte[] input = "test".getBytes();
        byte[] result = toBytes(false, input);
        assertArrayEquals("Byte array should return same array", input, result);
    }

    @Test
    public void toBytes_withString_returnsBytes()
    {
        String input = "test";
        byte[] result = toBytes(false, input);
        assertArrayEquals("String should be converted to bytes", input.getBytes(), result);
    }

    @Test
    public void toBytes_withText_returnsBytes()
    {
        org.apache.arrow.vector.util.Text input = new org.apache.arrow.vector.util.Text("test");
        byte[] result = toBytes(false, input);
        assertArrayEquals("Text should be converted to bytes", "test".getBytes(), result);
    }

    @Test
    public void toBytes_withNativeInteger_returnsBytes()
    {
        assertToBytesNativeType(123, 4, "Integer should be 4 bytes");
    }

    @Test
    public void toBytes_withNativeLong_returnsBytes()
    {
        assertToBytesNativeType(12345L, 8, "Long should be 8 bytes");
    }

    @Test
    public void toBytes_withNativeFloat_returnsBytes()
    {
        assertToBytesNativeType(1.23F, 4, "Float should be 4 bytes");
    }

    @Test
    public void toBytes_withNativeDouble_returnsBytes()
    {
        assertToBytesNativeType(1.23D, 8, "Double should be 8 bytes");
    }

    @Test
    public void toBytes_withNativeBoolean_returnsBytes()
    {
        assertToBytesNativeType(true, 1, "Boolean should be 1 byte");
    }

    private void assertToBytesNativeType(Object input, int expectedLength, String message)
    {
        byte[] result = toBytes(true, input);
        assertNotNull("Result should not be null", result);
        assertEquals(message, expectedLength, result.length);
    }

    @Test
    public void toBytes_withNonNativeInteger_returnsStringBytes()
    {
        Integer input = 123;
        byte[] result = toBytes(false, input);
        assertArrayEquals("Non-native integer should be converted to string bytes", "123".getBytes(), result);
    }

    @Test
    public void coerceType_withUnsupportedType_throwsIllegalArgumentException()
    {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                coerceType(false, Types.MinorType.TIMESTAMPMILLI.getType(), "test".getBytes()));
        assertTrue("Exception message should contain not supported", ex.getMessage().contains("not supported"));
    }

    @Test
    public void toBytes_withUnsupportedType_throwsRuntimeException()
    {
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                toBytes(true, new Object()));
        assertTrue("Exception message should contain Unsupported", ex.getMessage().contains("Unsupported"));
    }
}
