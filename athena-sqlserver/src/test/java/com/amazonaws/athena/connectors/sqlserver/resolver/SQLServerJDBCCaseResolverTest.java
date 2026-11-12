/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
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

public class SQLServerJDBCCaseResolverTest extends TestBase
{
    private static final String SCHEMA_NAME = "oRaNgE";
    private static final String TABLE_NAME = "ApPlE";

    private Connection mockConnection;
    private PreparedStatement preparedStatement;
    private DefaultJDBCCaseResolver resolver;

    @Before
    public void setup() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
        resolver = new SQLServerJDBCCaseResolver("sqlserver");
    }

    @Test
    public void getAdjustedName_caseInsensitiveSearchOnName_returnsAdjustedNames() throws SQLException {
            // Mock schema name result
            String[] schemaCols = {"SCHEMA_NAME"};
            int[] schemaTypes = {Types.VARCHAR};
            Object[][] schemaData = {{SCHEMA_NAME.toLowerCase()}};

            ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));
            when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

            String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(SCHEMA_NAME.toLowerCase(), adjustedSchemaName);

            // Mock table name result
            String[] tableCols = {"TABLE_NAME"};
            int[] tableTypes = {Types.VARCHAR};
            Object[][] tableData = {{TABLE_NAME.toUpperCase()}};

            ResultSet tableResultSet = mockResultSet(tableCols, tableTypes, tableData, new AtomicInteger(-1));
            when(preparedStatement.executeQuery()).thenReturn(tableResultSet);

            String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME, TABLE_NAME, Map.of(
                    CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(TABLE_NAME.toUpperCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedTableNameObject_caseInsensitiveSearchOnObject_returnsAdjustedTableNameObject() throws SQLException {
            // Mock schema and table result sets
            ResultSet schemaResultSet = mockResultSet(
                    new String[]{"SCHEMA_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{{SCHEMA_NAME.toLowerCase()}},
                    new AtomicInteger(-1));

            ResultSet tableResultSet = mockResultSet(
                    new String[]{"TABLE_NAME"},
                    new int[]{Types.VARCHAR},
                    new Object[][]{{TABLE_NAME.toUpperCase()}},
                    new AtomicInteger(-1));

            when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);

            TableName adjusted = resolver.getAdjustedTableNameObject(
                    mockConnection,
                    new TableName(SCHEMA_NAME, TABLE_NAME),
                    Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
            assertEquals(new TableName(SCHEMA_NAME.toLowerCase(), TABLE_NAME.toUpperCase()), adjusted);
    }

    @Test
    public void getAdjustedName_lowerCaseMode_returnsLowerNames() {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.LOWER.name()));
        assertEquals(SCHEMA_NAME.toLowerCase(), adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME.toLowerCase(), TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.LOWER.name()));
        assertEquals(TABLE_NAME.toLowerCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedName_upperCaseMode_returnsUpperNames() {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.UPPER.name()));
        assertEquals(SCHEMA_NAME.toUpperCase(), adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME.toUpperCase(), TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.UPPER.name()));
        assertEquals(TABLE_NAME.toUpperCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedName_noneMode_returnsOriginalNames() {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(SCHEMA_NAME, adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME, TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(TABLE_NAME, adjustedTableName);
    }

    @Test
    public void getAdjustedName_missingCasingModeKey_returnsDefault() {
        // Default for SQLServer is NONE (passed in constructor)
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of());
        assertEquals(SCHEMA_NAME, adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME, TABLE_NAME, Map.of());
        assertEquals(TABLE_NAME, adjustedTableName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAdjustedName_invalidCasingModeValue_throwsException() {
        resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, "INVALID_MODE"));
    }
}
