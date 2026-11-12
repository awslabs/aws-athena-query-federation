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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCCaseResolverTest
{
    private static final String CASING_MODE_CONFIG_KEY = "casing_mode";
    private static final String CASING_MODE_LOWER = "LOWER";
    private static final String CASING_MODE_UPPER = "UPPER";
    private static final String CASING_MODE_CASE_INSENSITIVE_SEARCH = "CASE_INSENSITIVE_SEARCH";
    private static final String CASING_MODE_NONE = "NONE";
    private static final String CASING_MODE_ANNOTATION = "ANNOTATION";
    private static final String TEST_SCHEMA_NAME = "TestSchema";
    private static final String TEST_TABLE_NAME = "TestTable";
    private static final String TEST_SCHEMA_LOWER = "testschema";
    private static final String TEST_TABLE_LOWER = "testtable";
    private static final String TEST_SCHEMA_UPPER = "TESTSCHEMA";
    private static final String TEST_TABLE_UPPER = "TESTTABLE";
    private static final String DB_TYPE_TESTDB = "testdb";
    private static final String DB_TYPE_MYSQL = "mysql";
    private static final String DB_TYPE_UNSUPPORTED = "unsupportedtype";
    private TestJDBCCaseResolver caseResolver;
    private Connection mockConnection;
    private Map<String, String> configOptions;

    private static class TestJDBCCaseResolver extends JDBCCaseResolver
    {
        public TestJDBCCaseResolver(String sourceType)
        {
            super(sourceType);
        }

        public TestJDBCCaseResolver(String sourceType, FederationSDKCasingMode nonGlueBasedDefaultCasingMode, FederationSDKCasingMode glueConnectionBasedDefaultCasingMode)
        {
            super(sourceType, nonGlueBasedDefaultCasingMode, glueConnectionBasedDefaultCasingMode);
        }

        @Override
        protected List<String> doGetSchemaNameCaseInsensitively(Connection connection, String schemaNameInput, Map<String, String> configOptions)
        {
        if (TEST_SCHEMA_LOWER.equalsIgnoreCase(schemaNameInput)) {
            return List.of(TEST_SCHEMA_NAME);
            }
            return List.of();
        }

        @Override
        protected List<String> doGetTableNameCaseInsensitively(Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
        {
        if (TEST_TABLE_LOWER.equalsIgnoreCase(tableNameInput)) {
            return List.of(TEST_TABLE_NAME);
            }
            return List.of();
        }
    }

    @Before
    public void setUp()
    {
        caseResolver = new TestJDBCCaseResolver(DB_TYPE_TESTDB);
        mockConnection = Mockito.mock(Connection.class);
        configOptions = new HashMap<>();
    }

    @Test
    public void testConstructorWithSourceType()
    {
        JDBCCaseResolver resolver = new TestJDBCCaseResolver(DB_TYPE_MYSQL);
        Assert.assertNotNull(resolver);
    }

    @Test
    public void testConstructorWithCasingModes()
    {
        JDBCCaseResolver resolver = new TestJDBCCaseResolver(DB_TYPE_MYSQL,
            CaseResolver.FederationSDKCasingMode.LOWER,
            CaseResolver.FederationSDKCasingMode.UPPER);
        Assert.assertNotNull(resolver);
    }

    @Test
    public void testGetAdjustedSchemaNameStringLowerCase()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_LOWER);
        String result = caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_NAME, configOptions);
        Assert.assertEquals(TEST_SCHEMA_LOWER, result);
    }

    @Test
    public void testGetAdjustedSchemaNameStringUpperCase()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_UPPER);
        String result = caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_LOWER, configOptions);
        Assert.assertEquals(TEST_SCHEMA_UPPER, result);
    }

    @Test
    public void testGetAdjustedSchemaNameStringCaseInsensitive()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_CASE_INSENSITIVE_SEARCH);
        String result = caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_LOWER, configOptions);
        Assert.assertEquals(TEST_SCHEMA_NAME, result);
    }

    @Test
    public void testGetAdjustedSchemaNameStringNone()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_NONE);
        String result = caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_NAME, configOptions);
        Assert.assertEquals(TEST_SCHEMA_NAME, result);
    }

    @Test
    public void testGetAdjustedSchemaNameStringNoCasingMode()
    {
        String result = caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_NAME, configOptions);
        Assert.assertEquals(TEST_SCHEMA_NAME, result);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAdjustedSchemaNameStringAnnotationMode()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_ANNOTATION);
        caseResolver.getAdjustedSchemaNameString(mockConnection, TEST_SCHEMA_NAME, configOptions);
    }

    @Test
    public void testGetAdjustedTableNameStringLowerCase()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_LOWER);
        String result = caseResolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_NAME, configOptions);
        Assert.assertEquals(TEST_TABLE_LOWER, result);
    }

    @Test
    public void testGetAdjustedTableNameStringUpperCase()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_UPPER);
        String result = caseResolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_LOWER, configOptions);
        Assert.assertEquals(TEST_TABLE_UPPER, result);
    }

    @Test
    public void testGetAdjustedTableNameStringCaseInsensitive()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_CASE_INSENSITIVE_SEARCH);
        String result = caseResolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_LOWER, configOptions);
        Assert.assertEquals(TEST_TABLE_NAME, result);
    }

    @Test
    public void testGetAdjustedTableNameStringNone()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_NONE);
        String result = caseResolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_NAME, configOptions);
        Assert.assertEquals(TEST_TABLE_NAME, result);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAdjustedTableNameStringAnnotationMode()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_ANNOTATION);
        caseResolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, TEST_TABLE_NAME, configOptions);
    }

    @Test
    public void testGetAdjustedTableNameObjectLowerCase()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_LOWER);
        TableName inputTable = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        TableName result = caseResolver.getAdjustedTableNameObject(mockConnection, inputTable, configOptions);

        Assert.assertEquals(TEST_SCHEMA_LOWER, result.getSchemaName());
        Assert.assertEquals(TEST_TABLE_LOWER, result.getTableName());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetAdjustedTableNameObjectAnnotationModeUnsupportedType()
    {
        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_ANNOTATION);
        TableName inputTable = new TableName(TEST_SCHEMA_NAME, "TestTable@table=lower");
        caseResolver.getAdjustedTableNameObject(mockConnection, inputTable, configOptions);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testSchemaNameCaseInsensitivelyMultipleMatches()
    {
        TestJDBCCaseResolver resolver = new TestJDBCCaseResolver(DB_TYPE_TESTDB) {
            @Override
            protected List<String> doGetSchemaNameCaseInsensitively(Connection connection, String schemaNameInput, Map<String, String> configOptions)
            {
                return Arrays.asList("Schema1", "Schema2"); // Multiple matches
            }
        };

        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_CASE_INSENSITIVE_SEARCH);
        resolver.getAdjustedSchemaNameString(mockConnection, "schema", configOptions);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testTableNameCaseInsensitivelyMultipleMatches()
    {
        TestJDBCCaseResolver resolver = new TestJDBCCaseResolver(DB_TYPE_TESTDB) {
            @Override
            protected List<String> doGetTableNameCaseInsensitively(Connection connection, String schemaNameInCorrectCase, String tableNameInput, Map<String, String> configOptions)
            {
                return Arrays.asList("Table1", "Table2"); // Multiple matches
            }
        };

        configOptions.put(CASING_MODE_CONFIG_KEY, CASING_MODE_CASE_INSENSITIVE_SEARCH);
        resolver.getAdjustedTableNameString(mockConnection, TEST_SCHEMA_NAME, "table", configOptions);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaseInsensitivelySchemaNameQueryTemplate()
    {
        caseResolver.getCaseInsensitivelySchemaNameQueryTemplate();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetCaseInsensitivelySchemaNameColumnKey()
    {
        caseResolver.getCaseInsensitivelySchemaNameColumnKey();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDoGetSchemaNameCaseInsensitivelyDefaultImplementation()
    {
        JDBCCaseResolver defaultResolver = new JDBCCaseResolver(DB_TYPE_UNSUPPORTED) {
        };

        defaultResolver.doGetSchemaNameCaseInsensitively(mockConnection, "test", configOptions);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDoGetTableNameCaseInsensitivelyDefaultImplementation()
    {
        JDBCCaseResolver defaultResolver = new JDBCCaseResolver(DB_TYPE_UNSUPPORTED) {
        };

        defaultResolver.doGetTableNameCaseInsensitively(mockConnection, TEST_SCHEMA_NAME, "test", configOptions);
    }
}
