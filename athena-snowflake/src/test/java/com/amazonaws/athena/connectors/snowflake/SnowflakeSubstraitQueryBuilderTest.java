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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
    private SqlOperator mockSqlOperator;

    @Mock
    private SqlNode mockWhereClause;

    @Mock
    private Schema mockSchema;

    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA_NAME = "test_schema";
    private static final String TABLE = "test_table";
    private static final SqlDialect SNOWFLAKE_DIALECT = SnowflakeSqlDialect.DEFAULT;

    private String base64Plan;
    private List<Field> schemaFields;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        queryBuilder = new SnowflakeQueryStringBuilder("\"", null);

        base64Plan = Base64.getEncoder().encodeToString("mock_substrait_plan".getBytes());

        // Setup mock schema with concrete fields
        schemaFields = new ArrayList<>();
        schemaFields.add(new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schemaFields.add(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null));
        schemaFields.add(new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null));
        schemaFields.add(new Field("created_at", FieldType.nullable(new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)), null));
        when(mockSchema.getFields()).thenReturn(schemaFields);

        // Setup mock operator for SqlSelect
        when(mockSqlSelect.getOperator()).thenReturn(mockSqlOperator);
        when(mockSqlOperator.getKind()).thenReturn(SqlKind.SELECT);
    }

    /**
     * Test the most common success scenario for buildSplitSql where constraints.getQueryPlan()
     * is not null and leverages the prepareStatementWithSqlDialect path.
     */
    @Test
    void testBuildSplitSql_WithQueryPlan_Success() throws SQLException {
        // Arrange
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);
        when(mockSplit.getProperties()).thenReturn(new HashMap<>());
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        String expectedSql = "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\" WHERE \"id\" > 10";
        when(mockSqlSelect.getWhere()).thenReturn(mockWhereClause);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);
        when(mockSqlSelect.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, expectedSql));
        when(mockWhereClause.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, "\"id\" > 10"));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection, CATALOG, SCHEMA_NAME, TABLE, mockSchema, mockConstraints, mockSplit);

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Capture and verify the actual SQL passed to prepareStatement
            ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
            verify(mockConnection).prepareStatement(sqlCaptor.capture());
            String capturedSql = sqlCaptor.getValue();
            assertTrue(capturedSql.contains("SELECT"), "SQL should contain SELECT");
            assertTrue(capturedSql.contains("\"id\", \"name\", \"age\", \"created_at\""), "SQL should contain projected columns");
            assertTrue(capturedSql.contains("WHERE"), "SQL should contain WHERE clause");
            assertTrue(capturedSql.contains("\"id\" > 10"), "SQL should contain filter predicate");

            // Verify SubstraitSqlUtils was called with the specific SnowflakeSqlDialect
            mockedSubstraitUtils.verify(() ->
                SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)));
        }
    }

    /**
     * Test that buildSplitSql correctly uses SnowflakeSqlDialect when constraints has a query plan.
     */
    @Test
    void testBuildSplitSql_UsesSnowflakeSqlDialect() throws SQLException {
        // Arrange
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);
        when(mockSplit.getProperties()).thenReturn(new HashMap<>());
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        when(mockSqlSelect.getWhere()).thenReturn(null);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);
        when(mockSqlSelect.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\""));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection, CATALOG, SCHEMA_NAME, TABLE, mockSchema, mockConstraints, mockSplit);

            // Assert
            assertNotNull(result);

            // Verify the specific SnowflakeSqlDialect instance was used
            mockedSubstraitUtils.verify(() ->
                SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)));
        }
    }

    /**
     * Test that buildSplitSql properly handles parameter binding in the prepareStatementWithSqlDialect path.
     */
    @Test
    void testBuildSplitSql_WithParameterBinding() throws SQLException {
        // Arrange
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);
        when(mockSplit.getProperties()).thenReturn(new HashMap<>());
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        String expectedSql = "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\" WHERE \"name\" = ?";
        when(mockSqlSelect.getWhere()).thenReturn(mockWhereClause);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);
        when(mockSqlSelect.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, expectedSql));
        when(mockWhereClause.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, "\"name\" = ?"));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection, CATALOG, SCHEMA_NAME, TABLE, mockSchema, mockConstraints, mockSplit);

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Capture and verify the SQL contains expected table reference
            ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
            verify(mockConnection).prepareStatement(sqlCaptor.capture());
            String capturedSql = sqlCaptor.getValue();
            assertTrue(capturedSql.contains("SELECT"), "SQL should contain SELECT");
            assertTrue(capturedSql.contains("FROM"), "SQL should contain FROM");
            assertTrue(capturedSql.contains("\"test_schema\".\"test_table\""), "SQL should reference the correct table");
        }
    }

    /**
     * Test that buildSplitSql handles the case where query plan exists but no WHERE clause is present.
     */
    @Test
    void testBuildSplitSql_WithQueryPlan_NoWhereClause() throws SQLException {
        // Arrange
        when(mockQueryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(mockConstraints.getQueryPlan()).thenReturn(mockQueryPlan);
        when(mockSplit.getProperties()).thenReturn(new HashMap<>());
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        when(mockSqlSelect.getWhere()).thenReturn(null);
        when(mockSqlSelect.getOrderList()).thenReturn(null);
        when(mockSqlSelect.getFetch()).thenReturn(null);
        when(mockSqlSelect.getOffset()).thenReturn(null);
        when(mockSqlSelect.toSqlString(eq(SNOWFLAKE_DIALECT)))
                .thenReturn(new SqlString(SNOWFLAKE_DIALECT, "SELECT \"id\", \"name\", \"age\", \"created_at\" FROM \"test_schema\".\"test_table\""));

        try (MockedStatic<SubstraitSqlUtils> mockedSubstraitUtils = mockStatic(SubstraitSqlUtils.class)) {
            mockedSubstraitUtils.when(() -> SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(eq(base64Plan), eq(SNOWFLAKE_DIALECT)))
                    .thenReturn(mockSqlSelect);

            // Act
            PreparedStatement result = queryBuilder.buildSql(
                    mockConnection, CATALOG, SCHEMA_NAME, TABLE, mockSchema, mockConstraints, mockSplit);

            // Assert
            assertNotNull(result);
            assertEquals(mockPreparedStatement, result);

            // Capture and verify the SQL does not contain WHERE
            ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
            verify(mockConnection).prepareStatement(sqlCaptor.capture());
            String capturedSql = sqlCaptor.getValue();
            assertTrue(capturedSql.contains("SELECT"), "SQL should contain SELECT");
            assertTrue(capturedSql.contains("FROM"), "SQL should contain FROM");
            assertFalse(capturedSql.contains("WHERE"), "SQL should not contain WHERE clause");
        }
    }
}
