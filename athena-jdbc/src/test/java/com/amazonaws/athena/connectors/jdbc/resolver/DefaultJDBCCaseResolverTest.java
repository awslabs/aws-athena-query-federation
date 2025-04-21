/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.resolver.CaseResolver.CASING_MODE_CONFIGURATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultJDBCCaseResolverTest
{
    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;
    private ResultSet mockResultSet;
    private TestableHelper testableHelper;

    @Before
    public void setup()
    {
        mockConnection = Mockito.mock(Connection.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        mockResultSet = Mockito.mock(ResultSet.class);
        testableHelper = new TestableHelper("sourcetype");
    }

    @Test
    public void testDefaultJDBCCaseResolverDefaultCasing()
    {
        DefaultJDBCCaseResolver test = new DefaultJDBCCaseResolver("asdf", CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);

        String schemaName = "oRaNgE";
        String tableName = "ApPlE";

        // no glue connection based on config will be upper case based on config
        String nonGlueConnectionSchemaName = test.getAdjustedSchemaNameString(mockConnection, tableName, Map.of());
        assertEquals(tableName.toUpperCase(), nonGlueConnectionSchemaName);

        String nonGlueConnectionTableName = test.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of());
        assertEquals(tableName.toUpperCase(), nonGlueConnectionTableName);

        TableName nonGlueConnectionObject = test.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of());
        assertEquals(new TableName(schemaName.toUpperCase(), tableName.toUpperCase()), nonGlueConnectionObject);

        // glue connection based on config will be lowered case based on config
        String glueConnectionSchemaName = test.getAdjustedSchemaNameString(mockConnection, tableName, Map.of(DEFAULT_GLUE_CONNECTION, "asdf"));
        assertEquals(tableName.toLowerCase(), glueConnectionSchemaName);

        String glueConnectionTableName = test.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(DEFAULT_GLUE_CONNECTION, "asdf"));
        assertEquals(tableName.toLowerCase(), glueConnectionTableName);

        TableName glueConnectionObject = test.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(DEFAULT_GLUE_CONNECTION, "asdf"));
        assertEquals(new TableName(schemaName.toLowerCase(), tableName.toLowerCase()), glueConnectionObject);
    }

    @Test
    public void testNoneOverrideCase() {
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver test = new DefaultJDBCCaseResolver("asdf", CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);
        // no glue connection based on config will be upper case
        String schemaNameOutput = test.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(schemaName, schemaNameOutput);

        String tableNameOutput = test.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(tableName, tableNameOutput);

        TableName tableNameObjectOutput = test.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.NONE.name()));
        assertEquals(new TableName(schemaName, tableName), tableNameObjectOutput);
    }


    @Test
    public void testAnnotationOverrideCase() {
        // unsupported case
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver test = new DefaultJDBCCaseResolver("asdf", CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);

        // unsupported case
        assertThrows(UnsupportedOperationException.class, () -> test.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));
        assertThrows(UnsupportedOperationException.class, () -> test.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));
        assertThrows(UnsupportedOperationException.class, () ->  test.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));


        // only snowflake and synapse support annotation, we should not expand the usage of annotation casing resolving.
        DefaultJDBCCaseResolver saphana = new DefaultJDBCCaseResolver("saphana", CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);
        // it doesn't support on name level, only TableName object level.
        assertThrows(UnsupportedOperationException.class, () -> saphana.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));
        assertThrows(UnsupportedOperationException.class, () -> saphana.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));

        TableName adjustedTableNameObject = saphana.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name()));
        assertEquals(new TableName(schemaName, tableName), adjustedTableNameObject);

        //default snowflake case if no annotation is to upper case, this is for backward compatibility.
        String tableNameAnnotation = "ApPlE@schemaCase=upper&tableCase=lower";
        adjustedTableNameObject = saphana.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableNameAnnotation), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name()));
        assertEquals(new TableName(schemaName.toUpperCase(), tableName.toLowerCase()), adjustedTableNameObject);
    }

    @Test
    public void testCaseInsensitivelyOverrideCase()
    {
        // unsupported case
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver test = new DefaultJDBCCaseResolver("asdf", CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);

        // unsupported case
        assertThrows(UnsupportedOperationException.class, () -> test.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())));
        assertThrows(UnsupportedOperationException.class, () -> test.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())));
        assertThrows(UnsupportedOperationException.class, () ->  test.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name())));
    }

    @Test
    public void testSchemaNameCaseInsensitively() throws Exception {
        String inputSchema = "MySchema";
        String expectedSchema = "myschema";

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("schema_name")).thenReturn(expectedSchema);
        String result = testableHelper.getAdjustedSchemaNameString(mockConnection, inputSchema, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(expectedSchema, result);

        //Exception scenario
        when(mockResultSet.next()).thenReturn(true,true, false);
        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class, () ->
                testableHelper.getAdjustedSchemaNameString(mockConnection, inputSchema, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()))
        );
        assertTrue(ex.getMessage().contains("Schema name case insensitive match failed"));

        verify(mockPreparedStatement, Mockito.times(2)).setString(anyInt(), anyString());
        verify(mockPreparedStatement, Mockito.times(2)).close();
        verify(mockResultSet, Mockito.times(2)).close();
    }

    @Test
    public void testDoGetSchemaNameCaseInsensitively_sqlExceptionThrown() throws Exception {
        String input = "InvalidSchema";

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("DB error"));
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                testableHelper.doGetSchemaNameCaseInsensitively(mockConnection, input, Map.of())
        );

        assertTrue(ex.getMessage().contains("getSchemaNameCaseInsensitively query failed for"));
    }

    @Test
    public void testTableNameCaseInsensitively() throws Exception {
        String inputSchema = "MySchema";
        String inputTable = "Table1";
        String expectedTable = "table1";

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("table_name")).thenReturn(expectedTable);

        String result = testableHelper.getAdjustedTableNameString(mockConnection, inputSchema, inputTable, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(expectedTable, result);

        //Exception scenario
        when(mockResultSet.next()).thenReturn(true,true, false);
        AthenaConnectorException ex = assertThrows(AthenaConnectorException.class, () ->
                testableHelper.getAdjustedTableNameString(mockConnection, inputSchema, inputTable, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()))
        );
        assertTrue(ex.getMessage().contains("Table name case insensitive match failed"));

        //verify preparedStatement.setString will call 4 times (2 times for inputSchema and 2 times for inputTable)
        verify(mockPreparedStatement, Mockito.times(4)).setString(anyInt(), anyString());
        verify(mockPreparedStatement, Mockito.times(2)).close();
        verify(mockResultSet, Mockito.times(2)).close();
    }

    @Test
    public void testDoGetTableNameCaseInsensitively_sqlExceptionThrown() throws Exception {
        String inputSchema = "InvalidSchema";
        String inputTable = "InvalidTable";
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("DB error"));
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                testableHelper.doGetTableNameCaseInsensitively(mockConnection, inputSchema, inputTable, Map.of())
        );

        assertTrue(ex.getMessage().contains("getTableNameCaseInsensitively query failed for schema"));
    }

    // Subclass to override abstract/protected methods
    private static class TestableHelper extends DefaultJDBCCaseResolver {
        public TestableHelper(String sourceType) {
            super(sourceType, CaseResolver.FederationSDKCasingMode.UPPER, CaseResolver.FederationSDKCasingMode.LOWER);
        }

        @Override
        protected String getCaseInsensitivelySchemaNameQueryTemplate() {
            return "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
        }

        @Override
        protected String getCaseInsensitivelySchemaNameColumnKey() {
            return "schema_name";
        }

        @Override
        protected List<String> getCaseInsensitivelyTableNameQueryTemplate()
        {
            return List.of("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?");
        }

        @Override
        protected String getCaseInsensitivelyTableNameColumnKey()
        {
            return "table_name";
        }
    }

}
