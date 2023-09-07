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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connectors.hbase.HbaseSchemaUtils.coerceType;
import static com.amazonaws.athena.connectors.hbase.HbaseSchemaUtils.toBytes;
import static com.amazonaws.athena.connectors.hbase.TestUtils.makeResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseSchemaUtilsTest
{
    @Test
    public void inferSchema()
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

        Schema schema = HbaseSchemaUtils.inferSchema(mockConnection, tableName, numToScan);

        Map<String, Types.MinorType> actualFields = new HashMap<>();
        schema.getFields().stream().forEach(next -> actualFields.put(next.getName(), Types.getMinorTypeForArrowType(next.getType())));

        Map<String, Types.MinorType> expectedFields = new HashMap<>();
        TestUtils.makeSchema().build().getFields().stream()
                .forEach(next -> expectedFields.put(next.getName(), Types.getMinorTypeForArrowType(next.getType())));

        for (Map.Entry<String, Types.MinorType> nextExpected : expectedFields.entrySet()) {
            assertNotNull(actualFields.get(nextExpected.getKey()));
            assertEquals(nextExpected.getKey(), nextExpected.getValue(), actualFields.get(nextExpected.getKey()));
        }
        assertEquals(expectedFields.size(), actualFields.size());

        verify(mockConnection, times(1)).scanTable(any(), nullable(Scan.class), nullable(ResultProcessor.class));
        verify(mockScanner, times(1)).iterator();
    }

    @Test
    public void getQualifiedTableName()
    {
        String table = "table";
        String schema = "schema";
        String expected = "schema:table";
        String actual = HbaseSchemaUtils.getQualifiedTableName(new TableName(schema, table));
        assertEquals(expected, actual);
    }

    @Test
    public void getQualifiedTable()
    {
        String table = "table";
        String schema = "schema";
        org.apache.hadoop.hbase.TableName expected = org.apache.hadoop.hbase.TableName.valueOf(schema + ":" + table);
        org.apache.hadoop.hbase.TableName actual = HbaseSchemaUtils.getQualifiedTable(new TableName(schema, table));
        assertEquals(expected, actual);
    }

    @Test
    public void inferType()
    {
        assertEquals(Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("1"));
        assertEquals(Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("1000"));
        assertEquals(Types.MinorType.BIGINT, HbaseSchemaUtils.inferType("-1"));
        assertEquals(Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType("1.0"));
        assertEquals(Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType(".01"));
        assertEquals(Types.MinorType.FLOAT8, HbaseSchemaUtils.inferType("-.01"));
        assertEquals(Types.MinorType.VARCHAR, HbaseSchemaUtils.inferType("BDFKD"));
        assertEquals(Types.MinorType.VARCHAR, HbaseSchemaUtils.inferType(""));
    }

    @Test
    public void coerceTypeTest()
    {
        boolean isNative = false;
        assertEquals("asf", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "asf".getBytes()));
        assertEquals("2.0", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "2.0".getBytes()));
        assertEquals(1, coerceType(isNative, Types.MinorType.INT.getType(), "1".getBytes()));
        assertEquals(-1, coerceType(isNative, Types.MinorType.INT.getType(), "-1".getBytes()));
        assertEquals(1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), "1".getBytes()));
        assertEquals(-1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), "-1".getBytes()));
        assertEquals(1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), "1.1".getBytes()));
        assertEquals(-1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), "-1.1".getBytes()));
        assertEquals(1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), "1.1".getBytes()));
        assertEquals(-1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), "-1.1".getBytes()));
        assertArrayEquals("-1.1".getBytes(), (byte[]) coerceType(isNative, Types.MinorType.VARBINARY.getType(), "-1.1".getBytes()));
    }

    @Test
    public void coerceTypeNativeTest()
    {
        boolean isNative = true;
        assertEquals("asf", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "asf".getBytes()));
        assertEquals("2.0", coerceType(isNative, Types.MinorType.VARCHAR.getType(), "2.0".getBytes()));
        assertEquals(1, coerceType(isNative, Types.MinorType.INT.getType(), toBytes(isNative, 1)));
        assertEquals(-1, coerceType(isNative, Types.MinorType.INT.getType(), toBytes(isNative, -1)));
        assertEquals(1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), toBytes(isNative, 1L)));
        assertEquals(-1L, coerceType(isNative, Types.MinorType.BIGINT.getType(), toBytes(isNative, -1L)));
        assertEquals(1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), toBytes(isNative, 1.1F)));
        assertEquals(-1.1F, coerceType(isNative, Types.MinorType.FLOAT4.getType(), toBytes(isNative, -1.1F)));
        assertEquals(1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), toBytes(isNative, 1.1D)));
        assertEquals(-1.1D, coerceType(isNative, Types.MinorType.FLOAT8.getType(), toBytes(isNative, -1.1D)));
        assertArrayEquals("-1.1".getBytes(), (byte[]) coerceType(isNative, Types.MinorType.VARBINARY.getType(), "-1.1".getBytes()));
    }

    @Test
    public void extractColumnParts()
    {
        String[] parts = HbaseSchemaUtils.extractColumnParts("family:column");
        assertEquals("family", parts[0]);
        assertEquals("column", parts[1]);
    }
}
