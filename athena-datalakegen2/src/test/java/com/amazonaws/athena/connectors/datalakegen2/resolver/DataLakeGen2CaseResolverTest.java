/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants;
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

public class DataLakeGen2CaseResolverTest extends TestBase
{
    private Connection mockConnection;
    private PreparedStatement preparedStatement;

    @Before
    public void setUp() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
    }

    @Test
    public void getAdjustedSchemaNameString_CaseInsensitiveSearch_ReturnsLowerCasedSchemaAndTableName() throws SQLException
    {
        String schemaName = "TeStScHeMa";
        String tableName = "TeStTaBlE";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);
        mockSchemaAndTable(schemaName, tableName);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(schemaName.toLowerCase(), adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(tableName.toLowerCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedTableNameObject_CaseInsensitiveSearch_ReturnsLowerCasedTableNameObject() throws SQLException
    {
        String schemaName = "TeStScHeMa";
        String tableName = "TeStTaBlE";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);
        mockSchemaAndTable(schemaName, tableName);

        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(schemaName, tableName),
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(new TableName(schemaName.toLowerCase(), tableName.toLowerCase()), adjusted);
    }

    @Test
    public void getAdjustedSchemaNameString_NoMatch_ReturnsOriginalSchemaAndTableName() throws SQLException
    {
        String schemaName = "NonExistentSchema";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

        ResultSet emptyResultSet = mockResultSet(
                new String[]{"SCHEMA_NAME"},
                new int[]{Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(schemaName, adjustedSchemaName);

        String tableName = "NonExistentTable";
        ResultSet emptyTableResultSet = mockResultSet(
                new String[]{"TABLE_NAME"},
                new int[]{Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(emptyTableResultSet);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(tableName, adjustedTableName);
    }

    @Test
    public void getAdjustedSchemaNameString_EmptyConfig_ReturnsOriginalSchemaName() throws SQLException
    {
        String schemaName = "TestSchema";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

        ResultSet emptyResultSet = mockResultSet(
                new String[]{"SCHEMA_NAME"},
                new int[]{Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

        // Test with empty configuration map - should default to original name
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of());
        
        assertEquals(schemaName, adjustedSchemaName);
    }
    
    @Test
    public void getAdjustedTableNameObject_NoMatch_ReturnsOriginalTableNameObject() throws SQLException
    {
        String schemaName = "NonExistentSchema";
        String tableName = "NonExistentTable";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

        ResultSet emptyResultSet = mockResultSet(
                new String[]{"SCHEMA_NAME", "TABLE_NAME"},
                new int[]{Types.VARCHAR, Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(schemaName, tableName),
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(new TableName(schemaName, tableName), adjusted);
    }

    @Test
    public void getAdjustedTableNameObject_EmptyConfig_ReturnsOriginalTableNameObject() throws SQLException
    {
        String schemaName = "TestSchema";
        String tableName = "TestTable";
        DefaultJDBCCaseResolver resolver = new DataLakeGen2CaseResolver(DataLakeGen2Constants.NAME);

        ResultSet emptyResultSet = mockResultSet(
                new String[]{"SCHEMA_NAME", "TABLE_NAME"},
                new int[]{Types.VARCHAR, Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

        // Test with empty configuration map - should default to original name
        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(schemaName, tableName),
                Map.of());
        assertEquals(new TableName(schemaName, tableName), adjusted);
    }

    private void mockSchemaAndTable(String schemaName, String tableName) throws SQLException
    {
        String[] schemaCols = {"SCHEMA_NAME"};
        int[] schemaTypes = {Types.VARCHAR};
        Object[][] schemaData = {{schemaName.toLowerCase()}};
        ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));

        String[] tableCols = {"TABLE_NAME"};
        int[] tableTypes = {Types.VARCHAR};
        Object[][] tableData = {{tableName.toLowerCase()}};
        ResultSet tableResultSet = mockResultSet(tableCols, tableTypes, tableData, new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);
    }
}