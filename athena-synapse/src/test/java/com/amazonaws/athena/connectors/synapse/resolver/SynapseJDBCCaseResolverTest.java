/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.resolver.DefaultJDBCCaseResolver;
import com.amazonaws.athena.connectors.synapse.SynapseConstants;
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

public class SynapseJDBCCaseResolverTest extends TestBase
{
    private static final String TEST_SCHEMA_NAME = "oRaNgE";
    private static final String TEST_TABLE_NAME = "ApPlE";
    private static final String SCHEMA_COLUMN = "SCHEMA_NAME";
    private static final String TABLE_COLUMN = "TABLE_NAME";
    private static final String CASE_INSENSITIVE_MODE = CaseResolver.FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH.name();

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
    public void testCaseInsensitiveCaseOnName() throws SQLException {
        DefaultJDBCCaseResolver resolver = new SynapseJDBCCaseResolver(SynapseConstants.NAME);

        // Mock schema name result
        ResultSet schemaResultSet = mockResultSet(
                new String[]{SCHEMA_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{TEST_SCHEMA_NAME.toLowerCase()}},
                new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet);

        String adjustedSchemaName = resolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CASE_INSENSITIVE_MODE));
        assertEquals(TEST_SCHEMA_NAME.toLowerCase(), adjustedSchemaName);

        // Mock table name result
        ResultSet tableResultSet = mockResultSet(
                new String[]{TABLE_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{TEST_TABLE_NAME.toUpperCase()}},
                new AtomicInteger(-1));
        when(preparedStatement.executeQuery()).thenReturn(tableResultSet);

        String adjustedTableName = resolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_NAME, Map.of(
                CASING_MODE_CONFIGURATION_KEY, CASE_INSENSITIVE_MODE));
        assertEquals(TEST_TABLE_NAME.toUpperCase(), adjustedTableName);
    }

    @Test
    public void testCaseInsensitiveCaseOnObject() throws SQLException {
        DefaultJDBCCaseResolver resolver = new SynapseJDBCCaseResolver(SynapseConstants.NAME);

        // Mock schema and table result sets
        ResultSet schemaResultSet = mockResultSet(
                new String[]{SCHEMA_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{TEST_SCHEMA_NAME.toLowerCase()}},
                new AtomicInteger(-1));

        ResultSet tableResultSet = mockResultSet(
                new String[]{TABLE_COLUMN},
                new int[]{Types.VARCHAR},
                new Object[][]{{TEST_TABLE_NAME.toUpperCase()}},
                new AtomicInteger(-1));

        when(preparedStatement.executeQuery()).thenReturn(schemaResultSet).thenReturn(tableResultSet);

        TableName adjusted = resolver.getAdjustedTableNameObject(
                mockConnection,
                new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME),
                Map.of(CASING_MODE_CONFIGURATION_KEY, CASE_INSENSITIVE_MODE));
        assertEquals(new TableName(TEST_SCHEMA_NAME.toLowerCase(), TEST_TABLE_NAME.toUpperCase()), adjusted);
    }
}