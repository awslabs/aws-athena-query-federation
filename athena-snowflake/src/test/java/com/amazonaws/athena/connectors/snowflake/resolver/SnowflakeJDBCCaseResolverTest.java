/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.resolver.CaseResolver.CASING_MODE_CONFIGURATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class SnowflakeJDBCCaseResolverTest extends TestBase
{
    private Connection mockConnection;
    private PreparedStatement preparedStatement;

    @Before
    public void setup()
            throws SQLException
    {
        mockConnection = Mockito.mock(Connection.class);
        preparedStatement = Mockito.mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);
    }

    @Test
    public void testAnnotationOverrideCase() {
        // unsupported case
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver snowflake = new SnowflakeJDBCCaseResolver(SnowflakeConstants.SNOWFLAKE_NAME);

        // unsupported case
        assertThrows(UnsupportedOperationException.class, () -> snowflake.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));
        assertThrows(UnsupportedOperationException.class, () -> snowflake.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name())));

        //default snowflake case if no annotation is to upper case, this is for backward compatibility.
        TableName adjustedTableNameObject = snowflake.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name()));
        assertEquals(new TableName(schemaName.toUpperCase(), tableName.toUpperCase()), adjustedTableNameObject);

        String tableNameAnnotation = "ApPlE@schemaCase=upper&tableCase=lower";
        adjustedTableNameObject = snowflake.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableNameAnnotation), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.ANNOTATION.name()));
        assertEquals(new TableName(schemaName.toUpperCase(), tableName.toLowerCase()), adjustedTableNameObject);
    }

    @Test
    public void testCaseInsensitiveCaseOnName()
            throws SQLException
    {
        // unsupported case
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver snowflake = new SnowflakeJDBCCaseResolver(SnowflakeConstants.SNOWFLAKE_NAME);

        String[] columns = {"SCHEMA_NAME"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{schemaName.toLowerCase()}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String adjustedSchemaNameString = snowflake.getAdjustedSchemaNameString(mockConnection, schemaName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(schemaName.toLowerCase(), adjustedSchemaNameString);

        String[] columns1 = {"TABLE_NAME"};
        int[] types1 = {Types.VARCHAR};
        Object[][] values1 = {{tableName.toUpperCase()}};
        ResultSet resultSet1 = mockResultSet(columns1, types1, values1, new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(resultSet1);

        String adjustedTableNameString = snowflake.getAdjustedTableNameString(mockConnection, schemaName, tableName, Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(tableName.toUpperCase(), adjustedTableNameString);
    }

    @Test
    public void testCaseInsensitiveCaseOnObject()
            throws SQLException
    {
        // unsupported case
        String schemaName = "oRaNgE";
        String tableName = "ApPlE";
        DefaultJDBCCaseResolver snowflake = new SnowflakeJDBCCaseResolver(SnowflakeConstants.SNOWFLAKE_NAME);

        String[] columns = {"SCHEMA_NAME"};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{schemaName.toLowerCase()}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));

        String[] columns1 = {"TABLE_NAME"};
        int[] types1 = {Types.VARCHAR};
        Object[][] values1 = {{tableName.toUpperCase()}};
        ResultSet resultSet1 = mockResultSet(columns1, types1, values1, new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(resultSet).thenReturn(resultSet1);
        TableName adjustedTableNameObject = snowflake.getAdjustedTableNameObject(mockConnection, new TableName(schemaName, tableName), Map.of(CASING_MODE_CONFIGURATION_KEY, CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name()));
        assertEquals(new TableName(schemaName.toLowerCase(), tableName.toUpperCase()), adjustedTableNameObject);
    }
}
