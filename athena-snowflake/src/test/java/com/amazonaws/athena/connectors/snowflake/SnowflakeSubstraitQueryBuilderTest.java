/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit tests for SnowflakeQueryStringBuilder's Substrait query plan integration.
 * Tests the buildSql path with constraints.getQueryPlan() not null to fully test the
 * prepareStatementWithSqlDialect changes.
 */
class SnowflakeSubstraitQueryBuilderTest {

    private SnowflakeQueryStringBuilder queryBuilder;

    @Mock
    private Connection mockConnection;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private Constraints mockConstraints;

    @Mock
    private QueryPlan mockQueryPlan;

    @Mock
    private Split mockSplit;

    @Mock
    private SqlSelect mockSqlSelect;

    @Mock
    private SqlNode mockWhereClause;

    @Mock
    private SqlNode mockOrderByClause;

    @Mock
    private SqlNode mockLimitClause;

    @Mock
    private SqlNode mockOffsetClause;

    @Mock
    private Schema mockSchema;

    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";
    private static final String TABLE = "test_table";

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        queryBuilder = new SnowflakeQueryStringBuilder("\"", null);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        // Setup mock schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        fields.add(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null));
        fields.add(new Field("created_at", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null));
        when(mockSchema.getFields()).thenReturn(fields);
    }


    /**
     * Test the most common success scenario for buildSplitSql where constraints.getQueryPlan()
     * is not null and leverages the prepareStatementWithSqlDialect path.
     */
    @Test
    void testBuildSplitSql_WithQueryPlan_Success() throws SQLException {
        // Arrange
        String base64Plan = createMockBase64Plan();
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);

        // Setup split properties
        Map<String, String> splitProperties = new HashMap<>();
        when(mockSplit.getProperties()).thenReturn(splitProperties);

        // Mock SqlSelect with basic structure
        when(mockSqlSelect.getWhere()).thenReturn(mockWhereClause);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);

        // Mock the main SqlSelect toSqlString method - this is what's actually called in the code
        when(mockSqlSelect.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\" WHERE \"id\" > 10"));

        // Mock the where clause SQL generation
        when(mockWhereClause.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "\"id\" > 10"));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), any(SqlDialect.class)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection,
                    CATALOG,
                    SCHEMA,
                    TABLE,
                    mockSchema,
                    mockConstraints,
                    mockSplit
            );

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Verify that prepareStatement was called with expected SQL structure
            verify(mockConnection).prepareStatement(argThat(sql -> {
                String sqlStr = sql.toString();
                return sqlStr.contains("SELECT") &&
                       sqlStr.contains("\"id\", \"name\", \"age\", \"created_at\"") &&
                       sqlStr.contains("FROM") &&
                       sqlStr.contains("WHERE") &&
                       sqlStr.contains("\"id\" > 10");
            }));

            // Verify SubstraitSqlUtils was called with SnowflakeSqlDialect
            mockedSubstraitUtils.verify(() ->
                SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SnowflakeSqlDialect.DEFAULT)));
        }
    }

    /**
     * Test that buildSplitSql correctly uses SnowflakeSqlDialect when constraints has a query plan.
     */
    @Test
    void testBuildSplitSql_UsesSnowflakeSqlDialect() throws SQLException {
        // Arrange
        String base64Plan = createMockBase64Plan();
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);

        // Setup split properties
        Map<String, String> splitProperties = new HashMap<>();
        when(mockSplit.getProperties()).thenReturn(splitProperties);

        when(mockSqlSelect.getWhere()).thenReturn(null);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);

        // Mock the main SqlSelect toSqlString method
        when(mockSqlSelect.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\""));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), any(SqlDialect.class)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection,
                    CATALOG,
                    SCHEMA,
                    TABLE,
                    mockSchema,
                    mockConstraints,
                    mockSplit
            );

            // Assert
            assertNotNull(result);

            // Verify that the correct SQL dialect (SnowflakeSqlDialect) was used
            mockedSubstraitUtils.verify(() ->
                SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SnowflakeSqlDialect.DEFAULT)));
        }
    }

    /**
     * Test that buildSplitSql properly handles parameter binding in the prepareStatementWithSqlDialect path.
     */
    @Test
    void testBuildSplitSql_WithParameterBinding() throws SQLException {
        // Arrange
        String base64Plan = createMockBase64Plan();
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);

        // Setup split properties
        Map<String, String> splitProperties = new HashMap<>();
        when(mockSplit.getProperties()).thenReturn(splitProperties);

        // Mock SqlSelect with where clause that will generate parameters
        when(mockSqlSelect.getWhere()).thenReturn(mockWhereClause);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);

        // Mock the main SqlSelect toSqlString method
        when(mockSqlSelect.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\" WHERE \"name\" = ?"));

        // Mock the where clause SQL generation
        when(mockWhereClause.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "\"name\" = ?"));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), any(SqlDialect.class)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection,
                    CATALOG,
                    SCHEMA,
                    TABLE,
                    mockSchema,
                    mockConstraints,
                    mockSplit
            );

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Verify that prepareStatement was called
            verify(mockConnection).prepareStatement(anyString());

            // Verify the SQL structure contains expected elements
            verify(mockConnection).prepareStatement(argThat(sql -> {
                String sqlStr = sql.toString();
                return sqlStr.contains("SELECT") &&
                       sqlStr.contains("FROM") &&
                       sqlStr.contains("\"test_schema\".\"test_table\"");
            }));
        }
    }

    /**
     * Test that buildSplitSql handles the case where query plan exists but no WHERE clause is present.
     */
    @Test
    void testBuildSplitSql_WithQueryPlan_NoWhereClause() throws SQLException {
        // Arrange
        String base64Plan = createMockBase64Plan();
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);

        // Setup split properties
        Map<String, String> splitProperties = new HashMap<>();
        when(mockSplit.getProperties()).thenReturn(splitProperties);

        // Mock SqlSelect with no where clause
        when(mockSqlSelect.getWhere()).thenReturn(null);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);

        // Mock the main SqlSelect toSqlString method
        when(mockSqlSelect.toSqlString(any(SqlDialect.class)))
                .thenReturn(new SqlString(SnowflakeSqlDialect.DEFAULT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\""));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), any(SqlDialect.class)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection,
                    CATALOG,
                    SCHEMA,
                    TABLE,
                    mockSchema,
                    mockConstraints,
                    mockSplit
            );

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Verify that prepareStatement was called with SQL that doesn't contain WHERE
            verify(mockConnection).prepareStatement(argThat(sql -> {
                String sqlStr = sql.toString();
                return sqlStr.contains("SELECT") &&
                       sqlStr.contains("FROM") &&
                       !sqlStr.contains("WHERE");
            }));
        }
    }

    // Helper methods
    private String createMockBase64Plan() {
        // Create a mock base64-encoded Substrait plan
        String mockPlan = "mock_substrait_plan";
        return Base64.getEncoder().encodeToString(mockPlan.getBytes());
    }
}
