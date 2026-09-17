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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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

    private static final String SCHEMA = "schema";
    private static final String CASE_INSENSITIVE_AMBIGUITY_MESSAGE = "Either no tables or multiple tables resolved from case insensitive name";

    @Test
    public void getQualifiedTableName_withTable_returnsQualifiedName()
    {
        String table = "table";
        testGetQualifiedTableName(table);
    }

    @Test
    public void getQualifiedTableName_withNamespacePrefix_returnsQualifiedName()
    {
        String table = "schema:table";
        testGetQualifiedTableName(table);
    }

    private void testGetQualifiedTableName(String table) {
        String expected = "schema:table";
        String actualWithTable = HbaseTableNameUtils.getQualifiedTableName(new TableName(SCHEMA, table));
        String actualWithStrings = HbaseTableNameUtils.getQualifiedTableName(SCHEMA, table);
        assertEquals("Qualified name from TableName should match", expected, actualWithTable);
        assertEquals("Qualified name from strings should match", expected, actualWithStrings);
    }

    @Test
    public void getQualifiedTable_withSchemaAndTable_returnsTableName()
    {
        String table = "table";
        org.apache.hadoop.hbase.TableName expected = org.apache.hadoop.hbase.TableName.valueOf(SCHEMA + ":" + table);
        org.apache.hadoop.hbase.TableName actualWithTable = HbaseTableNameUtils.getQualifiedTable(new TableName(SCHEMA, table));
        org.apache.hadoop.hbase.TableName actualWithStrings = HbaseTableNameUtils.getQualifiedTable(SCHEMA, table);
        assertEquals("Qualified TableName from TableName should match", expected, actualWithTable);
        assertEquals("Qualified TableName from strings should match", expected, actualWithStrings);
    }

    @Test
    public void getHbaseTableName_withCaseInsensitiveMatch_returnsResolvedTable()
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
        assertEquals("Resolved table should match expected", expected, result);
    }

    @Test
    public void getHbaseTableName_withFlagFalse_returnsQualifiedTableWithoutSearch()
            throws IOException
    {
        HBaseConnection mockConnection = mock(HBaseConnection.class);

        TableName input = new TableName("schema", "Test");
        org.apache.hadoop.hbase.TableName expected = HbaseTableNameUtils.getQualifiedTable("schema", "Test");
        org.apache.hadoop.hbase.TableName result = HbaseTableNameUtils.getHbaseTableName(com.google.common.collect.ImmutableMap.of(), mockConnection, input);
        assertEquals("Result should be qualified table without case search", expected, result);
        verify(mockConnection, times(0)).tableExists(any());
        verify(mockConnection, times(0)).listTableNamesByNamespace(any());
    }

    @Test
    public void getHbaseTableName_withNoMatchingTable_throwsIllegalStateException() {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:test")
        };
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);
        when(mockConnection.tableExists(any())).thenReturn(false);

        TableName input = new TableName("schema", "table");

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                HbaseTableNameUtils.getHbaseTableName(config, mockConnection, input));
        assertTrue("Exception message should describe case insensitive resolution",
                ex.getMessage().contains(CASE_INSENSITIVE_AMBIGUITY_MESSAGE));
    }

    @Test
    public void tryCaseInsensitiveSearch_withSingleMatch_returnsTable()
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
        assertEquals("Single match should return resolved table", expected, result);
    }

    @Test
    public void tryCaseInsensitiveSearch_withExactCaseMatch_returnsTable()
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
        assertEquals("Exact case match should return resolved table", expected, result);
    }

    @Test
    public void tryCaseInsensitiveSearch_withMultipleMatches_throwsIllegalStateException() {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:Test"),
            org.apache.hadoop.hbase.TableName.valueOf("schema:tEst")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input));
        assertTrue("Exception message should describe multiple tables",
                ex.getMessage().contains(CASE_INSENSITIVE_AMBIGUITY_MESSAGE));
    }

    @Test
    public void tryCaseInsensitiveSearch_withNoMatch_throwsIllegalStateException() {
        org.apache.hadoop.hbase.TableName[] tableNames = {
            org.apache.hadoop.hbase.TableName.valueOf("schema:other")
        };
        TableName input = new TableName("schema", "test");
        HBaseConnection mockConnection = mock(HBaseConnection.class);
        when(mockConnection.listTableNamesByNamespace(any())).thenReturn(tableNames);

        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                HbaseTableNameUtils.tryCaseInsensitiveSearch(mockConnection, input));
        assertTrue("Exception message should describe no tables resolved",
                ex.getMessage().contains(CASE_INSENSITIVE_AMBIGUITY_MESSAGE));
    }
}
