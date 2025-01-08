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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;

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
    public void getAdjustedTableObjectNameLower()
            throws Exception
    {
        TableName inputTableName = new TableName("testschema", "testtable");
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "lower");
        TableName outputTableName = OracleCaseResolver.getAdjustedTableObjectName(connection, inputTableName, config);
        Assert.assertEquals(outputTableName.getSchemaName(), inputTableName.getSchemaName());
        Assert.assertEquals(outputTableName.getTableName(), inputTableName.getTableName());
    }

    @Test
    public void getAdjustedTableObjectNameUpper()
            throws Exception
    {
        TableName inputTableName = new TableName("testschema", "testtable");
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "upper");
        TableName outputTableName = OracleCaseResolver.getAdjustedTableObjectName(connection, inputTableName, config);
        Assert.assertEquals(outputTableName.getSchemaName(), inputTableName.getSchemaName().toUpperCase());
        Assert.assertEquals(outputTableName.getTableName(), inputTableName.getTableName().toUpperCase());
    }

    @Test
    public void getAdjustedTableObjectNameSearch()
            throws Exception
    {
        String inputSchemaName = "testschema";
        String matchedSchemaName = "testSchema";
        String inputTableName = "testtable";
        String matchedTableName = "TESTTABLE";
        TableName inputTableNameObject = new TableName(inputSchemaName, inputTableName);
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "case_insensitive_search");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.TABLE_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columnsSchema = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] typesSchema = {Types.VARCHAR};
        Object[][] valuesSchema = {{matchedSchemaName}};
        ResultSet resultSetSchema = mockResultSet(columnsSchema, typesSchema, valuesSchema, new AtomicInteger(-1));
        String[] columnsTable = {OracleCaseResolver.TABLE_NAME_COLUMN_KEY};
        int[] typesTable = {Types.VARCHAR};
        Object[][] valuesTable = {{matchedTableName}};
        ResultSet resultSetTable = mockResultSet(columnsTable, typesTable, valuesTable, new AtomicInteger(-1));
        // schema query is first, then table name
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSetSchema).thenReturn(resultSetTable);
        
        TableName outputTableName = OracleCaseResolver.getAdjustedTableObjectName(connection, inputTableNameObject, config);
        Assert.assertEquals(outputTableName.getSchemaName(), matchedSchemaName);
        Assert.assertEquals(outputTableName.getTableName(), matchedTableName);
    }

    @Test
    public void getAdjustedTableObjectNameNoConfig()
            throws Exception
    {
        TableName inputTableName = new TableName("testschema", "testtable");
        Map<String, String> config = Collections.emptyMap();
        TableName outputTableName = OracleCaseResolver.getAdjustedTableObjectName(connection, inputTableName, config);
        Assert.assertEquals(outputTableName.getSchemaName(), inputTableName.getSchemaName().toUpperCase());
        Assert.assertEquals(outputTableName.getTableName(), inputTableName.getTableName().toUpperCase());
    }

    @Test
    public void getAdjustedTableObjectNameGlueConnection()
            throws Exception
    {
        TableName inputTableName = new TableName("testschema", "testtable");
        Map<String, String> config = Collections.singletonMap(DEFAULT_GLUE_CONNECTION, "notBlank");
        TableName outputTableName = OracleCaseResolver.getAdjustedTableObjectName(connection, inputTableName, config);
        Assert.assertEquals(outputTableName.getSchemaName(), inputTableName.getSchemaName());
        Assert.assertEquals(outputTableName.getTableName(), inputTableName.getTableName());
    }

    @Test
    public void getAdjustedSchemaNameLower()
            throws Exception
    {
        // the trino engine will lowercase anything
        String inputSchemaName = "testschema";
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "lower");
        String outputSchemaName = OracleCaseResolver.getAdjustedSchemaName(connection, inputSchemaName, config);
        Assert.assertEquals(inputSchemaName, outputSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameUpper()
            throws Exception
    {
        String inputSchemaName = "testschema";
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "upper");
        String outputSchemaName = OracleCaseResolver.getAdjustedSchemaName(connection, inputSchemaName, config);
        Assert.assertEquals(inputSchemaName.toUpperCase(), outputSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameSearch()
            throws Exception
    {
        String inputSchemaName = "testschema";
        String matchedSchemaName = "testSchema";
        Map<String, String> config = Collections.singletonMap(OracleCaseResolver.CASING_MODE, "case_insensitive_search");

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{matchedSchemaName}};
        ResultSet resultSet = mockResultSet(columns, types, values, new AtomicInteger(-1));
        Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
        
        String outputSchemaName = OracleCaseResolver.getAdjustedSchemaName(connection, inputSchemaName, config);
        Assert.assertEquals(matchedSchemaName, outputSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameNoConfig()
            throws Exception
    {
        String inputSchemaName = "testschema";
        Map<String, String> config = Collections.emptyMap();
        String outputSchemaName = OracleCaseResolver.getAdjustedSchemaName(connection, inputSchemaName, config);
        Assert.assertEquals(inputSchemaName.toUpperCase(), outputSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameGlueConnection()
            throws Exception
    {
        // the trino engine will lowercase anything
        String inputSchemaName = "testschema";
        Map<String, String> config = Collections.singletonMap(DEFAULT_GLUE_CONNECTION, "notBlank");
        String outputSchemaName = OracleCaseResolver.getAdjustedSchemaName(connection, inputSchemaName, config);
        Assert.assertEquals(inputSchemaName, outputSchemaName);
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

        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.SCHEMA_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.SCHEMA_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"testSchema"}, {"TESTSCHEMA"}};
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
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(OracleCaseResolver.TABLE_NAME_QUERY_TEMPLATE)).thenReturn(preparedStatement);

        String[] columns = {OracleCaseResolver.TABLE_NAME_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{"testtable"}, {"TESTTABLE"}};
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

    @Test
    public void convertToLiteral()
    {
        String input = "teststring";
        String expectedOutput = "\'teststring\'";
        Assert.assertEquals(expectedOutput, OracleCaseResolver.convertToLiteral(input));
        Assert.assertEquals(expectedOutput, OracleCaseResolver.convertToLiteral(expectedOutput));
    }
}
