/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.oracle;

import static org.mockito.ArgumentMatchers.nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;

public class OracleCaseResolverTest
        extends TestBase
{
    private JdbcConnectionFactory jdbcConnectionFactory;
    private Connection connection;

    @Before
    public void setup()
            throws Exception
    {
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class, Mockito.RETURNS_DEEP_STUBS);
        this.connection = Mockito.mock(Connection.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(JdbcCredentialProvider.class))).thenReturn(this.connection);
    }

    @Test
    public void getSchemaNameCaseInsensitively()
            throws Exception
    {
        String inputSchemaName = "tEsTsChEmA";
        String matchedSchemaName = "testSchema";

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{matchedSchemaName}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String outputSchemaName = OracleCaseResolver.getSchemaNameCaseInsensitively(connection, inputSchemaName);
        Assert.assertEquals(outputSchemaName, matchedSchemaName);
    }

    @Test(expected = RuntimeException.class)
    public void getSchemaNameCaseInsensitivelyFailMultipleMatches()
            throws Exception
    {
        String inputSchemaName = "tEsTsChEmA";
        String[] matchedSchemaName = {"testSchema", "TESTSCHEMA"};

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {matchedSchemaName};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // should throw exception because 2 matches found
        OracleCaseResolver.getSchemaNameCaseInsensitively(connection, inputSchemaName);
    }

    @Test(expected = RuntimeException.class)
    public void getSchemaNameCaseInsensitivelyFailNoMatches()
            throws Exception
    {
        String inputSchemaName = "tEsTsChEmA";
        String[] matchedSchemaName = {};

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {matchedSchemaName};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // should throw exception because no matches found
        OracleCaseResolver.getSchemaNameCaseInsensitively(connection, inputSchemaName);
    }

    @Test
    public void getTableNameCaseInsensitively()
            throws Exception
    {
        String schemaName = "TestSchema";
        String inputTableName = "tEsTtAbLe";
        String matchedTableName = "TestTable";
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.TABLE_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.TABLE_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{matchedTableName}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        String outputTableName = OracleCaseResolver.getTableNameCaseInsensitively(connection, schemaName, inputTableName);
        Assert.assertEquals(outputTableName, matchedTableName);
    }

    @Test(expected = RuntimeException.class)
    public void getTableNameCaseInsensitivelyFailMultipleMatches()
            throws Exception
    {
        String schemaName = "TestSchema";
        String inputTableName = "tEsTtAbLe";
        String[] matchedTableName = {"testtable", "TESTTABLE"};
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.TABLE_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.TABLE_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {matchedTableName};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // should throw exception because 2 matches found
        OracleCaseResolver.getTableNameCaseInsensitively(connection, schemaName, inputTableName);
    }

    @Test(expected = RuntimeException.class)
    public void getTableNameCaseInsensitivelyFailNoMatches()
            throws Exception
    {
        String schemaName = "TestSchema";
        String inputTableName = "tEsTtAbLe";
        String[] matchedTableName = {};
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.TABLE_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.TABLE_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {matchedTableName};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);

        // should throw exception because no matches found
        OracleCaseResolver.getTableNameCaseInsensitively(connection, schemaName, inputTableName);
    }
}
