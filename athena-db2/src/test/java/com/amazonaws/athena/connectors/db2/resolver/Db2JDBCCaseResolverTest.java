/*-
 * #%L
 * athena-db2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.db2.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.db2.Db2Constants;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.resolver.CaseResolver.CASING_MODE_CONFIGURATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class Db2JDBCCaseResolverTest extends TestBase
{
    private Connection mockConnection;
    private PreparedStatement preparedStatement;

    @Before
    public void setup() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
    }

    @Test
    public void testCaseInsensitiveCaseOnName() throws SQLException
    {
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver resolver = new Db2JDBCCaseResolver(Db2Constants.NAME);

        // Mock schema name result
        String[] schemaCols = {"SCHEMANAME"};
        int[] schemaTypes = {Types.VARCHAR};
        Object[][] schemaData = {{schemaName.toLowerCase()}};

        ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(schemaName.toLowerCase(), adjustedSchemaName);

        // Mock table name result
        String[] tableCols = {"TABNAME"};
        int[] tableTypes = {Types.VARCHAR};
        Object[][] tableData = {{tableName.toUpperCase()}};

        ResultSet tableResultSet = mockResultSet(tableCols, tableTypes, tableData, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(tableResultSet);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(tableName.toUpperCase(), adjustedTableName);
    }

    @Test
    public void testCaseInsensitiveCaseOnObject() throws SQLException
    {
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver resolver = new Db2JDBCCaseResolver(Db2Constants.NAME);

        // Mock schema and table result sets
        ResultSet schemaResultSet = mockResultSet(
                new String[]{"SCHEMANAME"},
                new int[]{Types.VARCHAR},
                new Object[][]{{schemaName.toLowerCase()}},
                new AtomicInteger(-1));

        ResultSet tableResultSet = mockResultSet(
                new String[]{"TABNAME"},
                new int[]{Types.VARCHAR},
                new Object[][]{{tableName.toUpperCase()}},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);

        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(schemaName, tableName),
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(new TableName(schemaName.toLowerCase(), tableName.toUpperCase()), adjusted);
    }
}
