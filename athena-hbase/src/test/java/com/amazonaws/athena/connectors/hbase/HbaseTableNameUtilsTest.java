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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;

public class HbaseTableNameUtilsTest
{
    private final Map<String, String> config = com.google.common.collect.ImmutableMap.of(HbaseTableNameUtils.ENABLE_CASE_INSENSITIVE_MATCH, "true");

    @Test
    public void getQualifiedTableName()
    {
        String table = "table";
        testGetQualifiedTableName(table);
    }

    @Test
    public void getQualifiedTableNameWithNamespace()
    {
        String table = "schema:table";
        testGetQualifiedTableName(table);
    }

    private void testGetQualifiedTableName(String table) {
        String schema = "schema";
        String expected = "schema:table";
        String actualWithTable = HbaseTableNameUtils.getQualifiedTableName(new TableName(schema, table));
        String actualWithStrings = HbaseTableNameUtils.getQualifiedTableName(schema, table);
        assertEquals(expected, actualWithTable);
        assertEquals(expected, actualWithStrings);
    }

    @Test
    public void getQualifiedTable()
    {
        String table = "table";
        String schema = "schema";
        org.apache.hadoop.hbase.TableName expected = org.apache.hadoop.hbase.TableName.valueOf(schema + ":" + table);
        org.apache.hadoop.hbase.TableName actualWithTable = HbaseTableNameUtils.getQualifiedTable(new TableName(schema, table));
        org.apache.hadoop.hbase.TableName actualWithStrings = HbaseTableNameUtils.getQualifiedTable(schema, table);
        assertEquals(expected, actualWithTable);
        assertEquals(expected, actualWithStrings);
    }

    @Test
    public void getHbaseTableName()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:Test")
        };
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        when(mockConnection.tableExists(any())).thenReturn(false);

        TableName input = new TableName("schema", "test");
        org.apache.hadoop.hbase.TableName expected = HbaseTableNameUtils.getQualifiedTable("schema", "Test");
        org.apache.hadoop.hbase.TableName result = HbaseTableNameUtils.getHbaseTableName(config, mockConnection, input);
        assertEquals(expected, result);
    }

    @Test
    public void getHbaseTableNameFlagFalse()
            throws IOException
    {
        HBaseConnection mockConnection = mock(HBaseConnection.class);

        TableName input = new TableName("schema", "Test");
        org.apache.hadoop.hbase.TableName expected = HbaseTableNameUtils.getQualifiedTable("schema", "Test");
        org.apache.hadoop.hbase.TableName result = HbaseTableNameUtils.getHbaseTableName(com.google.common.collect.ImmutableMap.of(), mockConnection, input);
        assertEquals(expected, result);
        verify(mockConnection, times(0)).tableExists(any());
        verify(mockConnection, times(0)).listTableNamesByNamespace(any());
    }

    @Test(expected = IllegalStateException.class)
    public void getHbaseTableNameDNE()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:test")
        };
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        when(mockConnection.tableExists(any())).thenReturn(false);

        TableName input = new TableName("schema", "table");
        HbaseTableNameUtils.getHbaseTableName(config, mockConnection, input);
    }
    
    @Test
    public void tryCaseInsensitiveSearch()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:test")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        org.apache.hadoop.hbase.TableName result = HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input);
        org.apache.hadoop.hbase.TableName expected = HbaseTableNameUtils.getQualifiedTable("schema", "test");
        assertEquals(expected, result);
    }
    
    @Test
    public void tryCaseInsensitiveSearchSingle()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:Test")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        org.apache.hadoop.hbase.TableName result = HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input);
        org.apache.hadoop.hbase.TableName expected = HbaseTableNameUtils.getQualifiedTable("schema", "Test");
        assertEquals(expected, result);
    }
    
    @Test(expected = IllegalStateException.class)
    public void tryCaseInsensitiveSearchMultiple()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:Test"),
            org.apache.hadoop.hbase.TableName.valueOf("schema:tEst")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input);
    }
    
    @Test(expected = IllegalStateException.class)
    public void tryCaseInsensitiveSearchNone()
            throws IOException
    {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:other")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input);
    }
}
