/*-
 * #%L
 * athena-clickhouse
 * %%
 * Copyright (C) 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.clickhouse;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClickHouseUtilTest extends TestBase
{
    private static final String COLUMN_TABLE_SCHEM = "TABLE_SCHEM";
    private static final String COLUMN_TABLE_NAME = "TABLE_NAME";

    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE_1 = "table_one";
    private static final String TEST_TABLE_2 = "table_two";

    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;

    @Before
    public void setup() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    }

    @Test
    public void getTables_withValues_returnsTableNames() throws SQLException
    {
        String[] columns = {COLUMN_TABLE_NAME, COLUMN_TABLE_SCHEM};
        Object[][] values = {
                {TEST_TABLE_1, TEST_SCHEMA},
                {TEST_TABLE_2, TEST_SCHEMA}
        };
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        when(mockPreparedStatement.executeQuery()).thenReturn(resultSet);

        List<TableName> tables = ClickHouseUtil.getTables(mockConnection, TEST_SCHEMA);

        assertNotNull(tables);
        assertEquals(2, tables.size());
        assertEquals(new TableName(TEST_SCHEMA, TEST_TABLE_1), tables.get(0));
        assertEquals(new TableName(TEST_SCHEMA, TEST_TABLE_2), tables.get(1));
        verify(mockPreparedStatement).setString(1, TEST_SCHEMA);
    }

    @Test
    public void getTables_withEmptyValues_returnsEmptyList() throws SQLException
    {
        String[] columns = {COLUMN_TABLE_NAME, COLUMN_TABLE_SCHEM};
        Object[][] values = {};
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        when(mockPreparedStatement.executeQuery()).thenReturn(resultSet);

        List<TableName> tables = ClickHouseUtil.getTables(mockConnection, TEST_SCHEMA);

        assertNotNull(tables);
        assertEquals(0, tables.size());
    }

    @Test
    public void getTableMetadata_withValues_returnsTableNames() throws SQLException
    {
        String[] columns = {COLUMN_TABLE_NAME, COLUMN_TABLE_SCHEM};
        Object[][] values = {
                {TEST_TABLE_1, TEST_SCHEMA}
        };
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        when(mockPreparedStatement.executeQuery()).thenReturn(resultSet);

        List<TableName> tables = ClickHouseUtil.getTableMetadata(mockPreparedStatement, "Tables and Views");

        assertNotNull(tables);
        assertEquals(1, tables.size());
        assertEquals(new TableName(TEST_SCHEMA, TEST_TABLE_1), tables.get(0));
    }

    @Test
    public void getTableMetadata_withSqlException_returnsEmptyList() throws SQLException
    {
        when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Connection closed"));

        List<TableName> tables = ClickHouseUtil.getTableMetadata(mockPreparedStatement, "Tables and Views");

        assertNotNull(tables);
        assertEquals(0, tables.size());
    }

    @Test
    public void getSchemaTableName_withValues_returnsTableName() throws SQLException
    {
        String[] columns = {COLUMN_TABLE_SCHEM, COLUMN_TABLE_NAME};
        Object[][] values = {{TEST_SCHEMA, TEST_TABLE_1}};
        ResultSet resultSet = mockResultSet(columns, values, new AtomicInteger(-1));
        resultSet.next(); // Advance to first row (getSchemaTableName expects cursor to be on a row)

        TableName tableName = ClickHouseUtil.getSchemaTableName(resultSet);

        assertNotNull(tableName);
        assertEquals(TEST_SCHEMA, tableName.getSchemaName());
        assertEquals(TEST_TABLE_1, tableName.getTableName());
    }

    @Test(expected = SQLException.class)
    public void getTables_whenPrepareStatementThrowsSQLException_throwsSQLException() throws SQLException
    {
        when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("Failed to prepare statement"));
        ClickHouseUtil.getTables(mockConnection, TEST_SCHEMA);
    }

    @Test(expected = SQLException.class)
    public void getSchemaTableName_whenResultSetThrowsSQLException_throwsSQLException() throws SQLException
    {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getString(COLUMN_TABLE_SCHEM)).thenThrow(new SQLException("ResultSet closed"));
        ClickHouseUtil.getSchemaTableName(resultSet);
    }
}
