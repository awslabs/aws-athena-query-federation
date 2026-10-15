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
package com.amazonaws.athena.connectors.clickhouse.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
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

public class ClickhouseJDBCCaseResolverTest extends TestBase
{
    private static final String CLICKHOUSE_ENGINE = "clickhouse";
    private static final String SCHEMA_NAME = "MySchema";
    private static final String TABLE_NAME = "MyTable";
    private static final String SCHEMA_COLUMN = "schema_name";
    private static final String TABLE_COLUMN = "table_name";

    private Connection mockConnection;
    private PreparedStatement preparedStatement;
    private DefaultJDBCCaseResolver resolver;

    @Before
    public void setup() throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
        this.resolver = new ClickhouseJDBCCaseResolver(CLICKHOUSE_ENGINE);
    }

    @Test
    public void getAdjustedSchemaNameString_CaseInsensitiveSearch_ReturnsLowerCasedSchemaName() throws SQLException
    {
        String[] schemaCols = {SCHEMA_COLUMN};
        int[] schemaTypes = {Types.VARCHAR};
        Object[][] schemaData = {{SCHEMA_NAME.toLowerCase()}};
        ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(
                mockConnection,
                SCHEMA_NAME,
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())
        );

        assertEquals(SCHEMA_NAME.toLowerCase(), adjustedSchemaName);
    }

    @Test
    public void getAdjustedTableNameString_CaseInsensitiveSearch_ReturnsLowerCasedTableName() throws SQLException
    {
        String[] tableCols = {TABLE_COLUMN};
        int[] tableTypes = {Types.VARCHAR};
        Object[][] tableData = {{TABLE_NAME.toLowerCase()}};
        ResultSet tableResultSet = mockResultSet(tableCols, tableTypes, tableData, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(tableResultSet);

        String adjustedTableName = resolver.getAdjustedTableNameString(
                mockConnection,
                SCHEMA_NAME.toLowerCase(),
                TABLE_NAME,
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())
        );

        assertEquals(TABLE_NAME.toLowerCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedTableNameObject_CaseInsensitiveSearch_ReturnsLowerCasedTableNameObject() throws SQLException
    {
        ResultSet schemaResultSet = mockResultSet(
                new String[]{SCHEMA_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{SCHEMA_NAME.toLowerCase()}},
                new AtomicInteger(-1));

        ResultSet tableResultSet = mockResultSet(
                new String[]{TABLE_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{TABLE_NAME.toLowerCase()}},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);

        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(SCHEMA_NAME, TABLE_NAME),
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())
        );
        assertEquals(new TableName(SCHEMA_NAME.toLowerCase(), TABLE_NAME.toLowerCase()), adjusted);
    }

    @Test
    public void getAdjustedSchemaNameString_NoneMode_ReturnsOriginalSchemaAndTableName()
    {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(SCHEMA_NAME, adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME, TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(TABLE_NAME, adjustedTableName);
    }

    @Test
    public void getAdjustedName_LowerCaseMode_ReturnsLowerCasedNames()
    {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.LOWER.name()));
        assertEquals(SCHEMA_NAME.toLowerCase(), adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME.toLowerCase(), TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.LOWER.name()));
        assertEquals(TABLE_NAME.toLowerCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedName_UpperCaseMode_ReturnsUpperCasedNames()
    {
        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.UPPER.name()));
        assertEquals(SCHEMA_NAME.toUpperCase(), adjustedSchemaName);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, SCHEMA_NAME.toUpperCase(), TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.UPPER.name()));
        assertEquals(TABLE_NAME.toUpperCase(), adjustedTableName);
    }

    @Test
    public void getAdjustedSchemaNameString_EmptyConfig_ReturnsLowerCasedSchemaName() throws SQLException
    {
        // Default for ClickHouse schema is CASE_INSENSITIVE_SEARCH - mock DB response
        String[] schemaCols = {SCHEMA_COLUMN};
        int[] schemaTypes = {Types.VARCHAR};
        Object[][] schemaData = {{SCHEMA_NAME.toLowerCase()}};
        ResultSet schemaResultSet = mockResultSet(schemaCols, schemaTypes, schemaData, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of());
        assertEquals(SCHEMA_NAME.toLowerCase(), adjustedSchemaName);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAdjustedSchemaNameString_InvalidCasingModeValue_ThrowsException()
    {
        resolver.getAdjustedSchemaNameString(mockConnection, SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, "INVALID_MODE"));
    }

    @Test(expected = AthenaConnectorException.class)
    public void getAdjustedSchemaNameString_whenNoRowsReturned_throwsAthenaConnectorException()
            throws SQLException
    {
        ResultSet emptyResultSet = mockResultSet(
                new String[]{SCHEMA_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{},
                new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(emptyResultSet);

        resolver.getAdjustedSchemaNameString(
                mockConnection,
                SCHEMA_NAME,
                Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
    }
}
