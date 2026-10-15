/*-
 * #%L
 * athena-mysql
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.mysql.resolver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.resolver.CaseResolver.FederationSDKCasingMode;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.athena.connector.lambda.resolver.CaseResolver.CASING_MODE_CONFIGURATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class MySqlJDBCCaseResolverTest extends TestBase {

    // Test Constants
    private static final String MYSQL_SOURCE_TYPE = "mysql";
    private static final String CUSTOM_SOURCE_TYPE = "custom_mysql";
    
    // Query Template Constants
    private static final String EXPECTED_SCHEMA_QUERY_TEMPLATE = 
        "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = ?";
    private static final String EXPECTED_TABLE_QUERY_TEMPLATE = 
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND lower(table_name) = ?";
    
    // Column Key Constants
    private static final String EXPECTED_SCHEMA_COLUMN_KEY = "schema_name";
    private static final String EXPECTED_TABLE_COLUMN_KEY = "table_name";
    
    // Test Data Constants
    private static final String TEST_SCHEMA_INPUT = "TeSt_ScHeMa";
    private static final String TEST_SCHEMA_EXPECTED = "test_schema";
    private static final String TEST_TABLE_INPUT = "TeSt_TaBlE";
    private static final String TEST_TABLE_EXPECTED = "test_table";
    private static final String TEST_SCHEMA_UPPER_EXPECTED = "TEST_SCHEMA";
    
    // Error Message Constants
    private static final String ANNOTATION_NAME_LEVEL_ERROR_MESSAGE = 
        "casing mode `ANNOTATION` is not supported for Name level";
    private static final String ANNOTATION_MYSQL_TYPE_ERROR_MESSAGE = 
        "casing mode `ANNOTATION` is not supported for type: 'mysql'";

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    private MySqlJDBCCaseResolver resolver;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        resolver = new MySqlJDBCCaseResolver(MYSQL_SOURCE_TYPE);
        when(mockConnection.prepareStatement(any())).thenReturn(mockPreparedStatement);
    }

    @Test
    public void constructor_withSourceType_createsResolverInstance() {
        assertNotNull("Resolver should not be null after construction", resolver);
        assertEquals(MySqlJDBCCaseResolver.class, resolver.getClass());
    }

    @Test
    public void getCaseInsensitivelySchemaNameQueryTemplate_returnsSchemaNameQueryTemplate() {
        String actualTemplate = resolver.getCaseInsensitivelySchemaNameQueryTemplate();
        assertEquals(EXPECTED_SCHEMA_QUERY_TEMPLATE, actualTemplate);
    }

    @Test
    public void getCaseInsensitivelySchemaNameColumnKey_returnsSchemaNameColumnKey() {
        String actualKey = resolver.getCaseInsensitivelySchemaNameColumnKey();
        assertEquals(EXPECTED_SCHEMA_COLUMN_KEY, actualKey);
    }

    @Test
    public void getCaseInsensitivelyTableNameQueryTemplate_returnsTableNameQueryTemplateList() {
        List<String> expectedTemplates = List.of(EXPECTED_TABLE_QUERY_TEMPLATE);
        List<String> actualTemplates = resolver.getCaseInsensitivelyTableNameQueryTemplate();

        assertThat(actualTemplates)
            .isNotNull()
            .hasSize(1)
            .containsExactly(EXPECTED_TABLE_QUERY_TEMPLATE)
            .isEqualTo(expectedTemplates);
    }

    @Test
    public void getCaseInsensitivelyTableNameColumnKey_returnsTableNameColumnKey() {
        String actualKey = resolver.getCaseInsensitivelyTableNameColumnKey();
        assertEquals(EXPECTED_TABLE_COLUMN_KEY, actualKey);
    }

    @Test
    public void getAdjustedSchemaNameString_withCaseInsensitiveSearch_returnsLowercaseSchemaName() throws SQLException {
        ResultSet mockResultSet = createMockSchemaResultSet();
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
                mockConnection,
                TEST_SCHEMA_INPUT,
                configOptions
        );

        assertEquals(TEST_SCHEMA_EXPECTED, actualSchemaName);
    }

    @Test
    public void getAdjustedTableNameString_withCaseInsensitiveSearch_returnsLowercaseTableName() throws SQLException {
        ResultSet mockResultSet = createMockTableResultSet();
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH);

        String actualTableName = resolver.getAdjustedTableNameString(
                mockConnection,
                TEST_SCHEMA_EXPECTED,
                TEST_TABLE_INPUT,
                configOptions
        );

        assertEquals(TEST_TABLE_EXPECTED, actualTableName);
    }

    @Test
    public void getAdjustedTableNameObject_withCaseInsensitiveSearch_returnsLowercaseTableNameObject() throws SQLException {
        TableName inputTableNameObj = new TableName(TEST_SCHEMA_INPUT, TEST_TABLE_INPUT);
        TableName expectedTableNameObj = new TableName(TEST_SCHEMA_EXPECTED, TEST_TABLE_EXPECTED);

        ResultSet schemaResultSet = createMockSchemaResultSet();
        ResultSet tableResultSet = createMockTableResultSet();

        when(mockPreparedStatement.executeQuery())
                .thenReturn(schemaResultSet)
                .thenReturn(tableResultSet);

        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.CASE_INSENSITIVE_SEARCH);

        TableName actualTableNameObj = resolver.getAdjustedTableNameObject(
                mockConnection,
                inputTableNameObj,
                configOptions
        );

        assertThat(actualTableNameObj)
                .isEqualTo(expectedTableNameObj)
                .satisfies(tableName -> {
                    assertEquals(TEST_SCHEMA_EXPECTED, tableName.getSchemaName());
                    assertEquals(TEST_TABLE_EXPECTED, tableName.getTableName());
                });
    }

    @Test
    public void getAdjustedSchemaNameString_withNoneCasingMode_returnsOriginalName() {
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.NONE);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
            mockConnection, 
            TEST_SCHEMA_INPUT, 
            configOptions
        );

        assertEquals(TEST_SCHEMA_INPUT, actualSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameString_withNoneCasingModeAndLowercase_returnsOriginalName() {
        String lowercaseInput = "lowercase_schema";
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.NONE);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
            mockConnection, 
            lowercaseInput, 
            configOptions
        );

        assertEquals(lowercaseInput, actualSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameString_withNoneCasingModeAndUppercase_returnsOriginalName() {
        String uppercaseInput = "UPPERCASE_SCHEMA";
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.NONE);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
            mockConnection, 
            uppercaseInput, 
            configOptions
        );

        assertEquals(uppercaseInput, actualSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameString_withLowerCasingMode_returnsLowercaseName() {
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.LOWER);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
            mockConnection, 
            TEST_SCHEMA_INPUT, 
            configOptions
        );

        assertEquals(TEST_SCHEMA_EXPECTED, actualSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameString_withUpperCasingMode_returnsUppercaseName() {
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.UPPER);

        String actualSchemaName = resolver.getAdjustedSchemaNameString(
            mockConnection, 
            TEST_SCHEMA_INPUT, 
            configOptions
        );

        assertEquals(TEST_SCHEMA_UPPER_EXPECTED, actualSchemaName);
    }

    @Test
    public void getAdjustedSchemaNameString_withAnnotationCasingMode_throwsUnsupportedOperationException() {
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.ANNOTATION);

        assertThatThrownBy(() -> resolver.getAdjustedSchemaNameString(
            mockConnection, 
            TEST_SCHEMA_EXPECTED, 
            configOptions
        ))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(ANNOTATION_NAME_LEVEL_ERROR_MESSAGE);
    }

    @Test
    public void getAdjustedTableNameString_withAnnotationCasingMode_throwsUnsupportedOperationException() {
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.ANNOTATION);

        assertThatThrownBy(() -> resolver.getAdjustedTableNameString(
            mockConnection, 
            TEST_SCHEMA_EXPECTED, 
            TEST_TABLE_EXPECTED, 
            configOptions
        ))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(ANNOTATION_NAME_LEVEL_ERROR_MESSAGE);
    }

    @Test
    public void getAdjustedTableNameObject_withAnnotationCasingMode_throwsUnsupportedOperationException() {
        TableName inputTableName = new TableName(TEST_SCHEMA_EXPECTED, TEST_TABLE_EXPECTED);
        Map<String, String> configOptions = createConfigOptions(FederationSDKCasingMode.ANNOTATION);

        assertThatThrownBy(() -> resolver.getAdjustedTableNameObject(
            mockConnection, 
            inputTableName, 
            configOptions
        ))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(ANNOTATION_MYSQL_TYPE_ERROR_MESSAGE);
    }

    @Test
    public void constructor_withCustomSourceType_createsResolverInstance() {
        MySqlJDBCCaseResolver customResolver = new MySqlJDBCCaseResolver(CUSTOM_SOURCE_TYPE);

        assertNotNull("Resolver should not be null when created with custom source type", customResolver);
        assertEquals(MySqlJDBCCaseResolver.class, customResolver.getClass());
    }

    @Test
    public void constructor_withMysqlSourceType_createsResolverInstance() {
        MySqlJDBCCaseResolver mysqlResolver = new MySqlJDBCCaseResolver(MYSQL_SOURCE_TYPE);

        assertNotNull("Resolver should not be null when created with mysql source type", mysqlResolver);
        assertEquals(MySqlJDBCCaseResolver.class, mysqlResolver.getClass());
    }

    @Test
    public void constructor_withNullSourceType_createsResolverInstance() {
        MySqlJDBCCaseResolver resolverWithNullType = new MySqlJDBCCaseResolver(null);

        assertNotNull("Resolver should not be null when created with null source type", resolverWithNullType);
        assertEquals(MySqlJDBCCaseResolver.class, resolverWithNullType.getClass());
    }

    // Helper Methods
    private Map<String, String> createConfigOptions(FederationSDKCasingMode casingMode) {
        return Map.of(CASING_MODE_CONFIGURATION_KEY, casingMode.name());
    }

    private ResultSet createMockSchemaResultSet() throws SQLException {
        String[] columns = {EXPECTED_SCHEMA_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{TEST_SCHEMA_EXPECTED}};
        return mockResultSet(columns, types, values, new AtomicInteger(-1));
    }

    private ResultSet createMockTableResultSet() throws SQLException {
        String[] columns = {EXPECTED_TABLE_COLUMN_KEY};
        int[] types = {Types.VARCHAR};
        Object[][] values = {{TEST_TABLE_EXPECTED}};
        return mockResultSet(columns, types, values, new AtomicInteger(-1));
    }
}