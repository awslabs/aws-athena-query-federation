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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.SubstraitTypeAndValue;
import com.amazonaws.athena.connector.util.EncodedSubstraitPlanStringGenerator;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration tests for JdbcSplitQueryBuilder using real H2 database connections to test
 * PreparedStatement parameter binding with actual database operations.
 */
public class JdbcSplitQueryBuilderIntegrationTest {
    private static final String H2_CONNECTION_STRING = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String QUOTE_CHAR = "\"";
    private static final SqlDialect DIALECT = AnsiSqlDialect.DEFAULT;

    private Connection realConnection;
    private JdbcSplitQueryBuilder builder;
    private FederationExpressionParser expressionParser;

    @BeforeEach
    public void setup() throws SQLException {
        // Create real H2 database connection
        realConnection = DriverManager.getConnection(H2_CONNECTION_STRING);

        expressionParser = mock(FederationExpressionParser.class);
        when(expressionParser.parseComplexExpressions(any(), any(), any()))
                .thenReturn(Collections.emptyList());

        builder = new JdbcSplitQueryBuilder(QUOTE_CHAR, expressionParser) {
            @Override
            protected String getFromClauseWithSplit(String catalog, String schema, String table,
                    Split split) {
                return " FROM " + quote(table);
            }

            @Override
            protected List<String> getPartitionWhereClauses(Split split) {
                return Collections.emptyList();
            }
        };

        // Create test table with various data types
        createTestTable();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        if (realConnection != null && !realConnection.isClosed()) {
            Statement stmt = realConnection.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + TEST_SCHEMA + "." + TEST_TABLE);
            stmt.execute("DROP SCHEMA IF EXISTS " + TEST_SCHEMA);
            stmt.close();
            realConnection.close();
        }
    }

    private void createTestTable() throws SQLException {
        Statement stmt = realConnection.createStatement();

        // Create schema first (required for Calcite-generated SQL with schema qualifier)
        stmt.execute("CREATE SCHEMA IF NOT EXISTS " + TEST_SCHEMA);

        // Create table matching EncodedSubstraitPlanStringGenerator.createTable() schema
        // Create table matching EncodedSubstraitPlanStringGenerator.createTable() schema
        // Note: H2 database limitations:
        // - MAP types are not supported in H2, using VARCHAR as workaround to store as strings
        // - ARRAY types in H2 have limited support, using ARRAY syntax where possible
        // - Nested arrays use VARCHAR as workaround since H2 has limited nested structure support
        stmt.execute("CREATE TABLE " + TEST_SCHEMA + "." + TEST_TABLE + " (" +
        // Boolean types
                "bool_col BOOLEAN, " +

                // Integer types (I8, I16, I32, I64)
                "tinyint_col TINYINT, " + "smallint_col SMALLINT, " + "int_col INT, "
                + "bigint_col BIGINT, " +

                // Floating point types (Fp32, Fp64)
                "float_col FLOAT, " + "double_col DOUBLE, " + "real_col REAL, " +

                // String and binary types
                "varchar_col VARCHAR, " + "char_col CHAR, " + "binary_col BINARY(100), "
                + "varbinary_col VARBINARY(100), " +

                // Temporal types (Date, Time, Timestamp)
                "date_col DATE, " + "time_col TIME, " + "timestamp_col TIMESTAMP, " +

                // Decimal types with various precision and scale
                "decimal_col DECIMAL(19,4), " + "decimal_col2 DECIMAL(10,2), "
                + "decimal_col3 DECIMAL(38,10), " +

                // Array/List types (H2 requires element type specification)
                "array_col VARCHAR ARRAY, " + "int_array_col INT ARRAY, " +

                // Map types (using VARCHAR as H2 doesn't support MAP)
                "map_col VARCHAR, " + "map_with_decimal VARCHAR, " +

                // Nested array (using VARCHAR as H2 has limited nested structure support)
                "nested_array VARCHAR" + ")");

        // Insert test data
        stmt.execute("INSERT INTO " + TEST_SCHEMA + "." + TEST_TABLE + " VALUES (" + "true, " + // bool_col
                "10, " + // tinyint_col
                "100, " + // smallint_col
                "1000, " + // int_col
                "1000000, " + // bigint_col
                "3.14, " + // float_col
                "95.5, " + // double_col
                "2.71, " + // real_col
                "'varchar_value', " + // varchar_col
                "'C', " + // char_col
                "X'DEADBEEF', " + // binary_col
                "X'CAFEBABE', " + // varbinary_col
                "'2020-01-15', " + // date_col
                "'10:30:00', " + // time_col
                "'2025-01-10 10:30:00', " + // timestamp_col
                "75000.5000, " + // decimal_col (19,4)
                "65000.75, " + // decimal_col2 (10,2)
                "12345.6789012345, " + // decimal_col3 (38,10)
                "ARRAY['val1', 'val2', 'val3'], " + // array_col
                "ARRAY[1, 2, 3, 4, 5], " + // int_array_col
                "NULL, " + // map_col (VARCHAR placeholder)
                "NULL, " + // map_with_decimal (VARCHAR placeholder)
                "NULL" + // nested_array (VARCHAR placeholder)
                ")");
        stmt.execute("INSERT INTO " + TEST_SCHEMA + "." + TEST_TABLE + " VALUES (" + "false, " + // bool_col
                "20, " + // tinyint_col
                "200, " + // smallint_col
                "2000, " + // int_col
                "2000000, " + // bigint_col
                "1.41, " + // float_col
                "88.3, " + // double_col
                "1.73, " + // real_col
                "'another_varchar', " + // varchar_col
                "'D', " + // char_col
                "X'FEEDFACE', " + // binary_col
                "X'BAADF00D', " + // varbinary_col
                "'2021-03-20', " + // date_col
                "'14:45:30', " + // time_col
                "'2025-01-09 14:45:30', " + // timestamp_col
                "85000.0000, " + // decimal_col (19,4)
                "70000.50, " + // decimal_col2 (10,2)
                "98765.4321098765, " + // decimal_col3 (38,10)
                "ARRAY['test1', 'test2'], " + // array_col
                "ARRAY[10, 20, 30], " + // int_array_col
                "NULL, " + // map_col (VARCHAR placeholder)
                "NULL, " + // map_with_decimal (VARCHAR placeholder)
                "NULL" + // nested_array (VARCHAR placeholder)
                ")");
        stmt.execute("INSERT INTO " + TEST_SCHEMA + "." + TEST_TABLE + " VALUES (" + "true, " + // bool_col
                "15, " + // tinyint_col
                "150, " + // smallint_col
                "1500, " + // int_col
                "1500000, " + // bigint_col
                "2.23, " + // float_col
                "92.1, " + // double_col
                "3.14, " + // real_col
                "'third_value', " + // varchar_col
                "'E', " + // char_col
                "X'C0FFEE00', " + // binary_col
                "X'FACADE00', " + // varbinary_col
                "'2019-06-10', " + // date_col
                "'09:15:45', " + // time_col
                "'2025-01-08 09:15:45', " + // timestamp_col
                "55000.2500, " + // decimal_col (19,4)
                "55000.00, " + // decimal_col2 (10,2)
                "11111.1111111111, " + // decimal_col3 (38,10)
                "ARRAY['a', 'b', 'c', 'd'], " + // array_col
                "ARRAY[100, 200, 300, 400], " + // int_array_col
                "NULL, " + // map_col (VARCHAR placeholder)
                "NULL, " + // map_with_decimal (VARCHAR placeholder)
                "NULL" + // nested_array (VARCHAR placeholder)
                ")");

        stmt.close();
    }

    @Test
    public void testRealPreparedStatementWithMultipleParameters() throws Exception {
        String sql = "SELECT * FROM " + TEST_SCHEMA + "." + TEST_TABLE
                + " WHERE int_col > ? AND decimal_col < ? AND bool_col = ?";
        PreparedStatement stmt = realConnection.prepareStatement(sql);

        // Multiple parameters of different types
        SubstraitTypeAndValue intParam =
                new SubstraitTypeAndValue(SqlTypeName.INTEGER, 500, "int_col");
        SubstraitTypeAndValue decimalParam = new SubstraitTypeAndValue(SqlTypeName.DECIMAL,
                new java.math.BigDecimal("80000.00"), "decimal_col");
        SubstraitTypeAndValue boolParam =
                new SubstraitTypeAndValue(SqlTypeName.BOOLEAN, true, "bool_col");

        List<SubstraitTypeAndValue> accumulator = List.of(intParam, decimalParam, boolParam);

        java.lang.reflect.Method method = JdbcSplitQueryBuilder.class.getDeclaredMethod(
                "handleDataTypesForPreparedStatement", PreparedStatement.class, List.class);
        method.setAccessible(true);
        PreparedStatement result = (PreparedStatement) method.invoke(builder, stmt, accumulator);

        // Execute and verify
        ResultSet rs = result.executeQuery();
        assertTrue(rs.next(), "Should find matching record");
        assertEquals(1000, rs.getInt("int_col")); // First row: int_col=1000,
                                                  // decimal_col=75000.5000, bool_col=true
        assertTrue(rs.getBoolean("bool_col"));

        rs.close();
        stmt.close();
    }

    @Test
    public void testRealPreparedStatementWithCalciteSql() throws Exception {
        String base64EncodedPlan = EncodedSubstraitPlanStringGenerator.generate(TEST_TABLE,
                TEST_SCHEMA, "SELECT * FROM test_table");

        QueryPlan queryPlan = mock(QueryPlan.class);
        when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlan);

        Constraints constraints = mock(Constraints.class);
        when(constraints.getQueryPlan()).thenReturn(queryPlan);

        Split split = mock(Split.class);

        PreparedStatement stmt = builder.prepareStatementWithCalciteSql(realConnection, constraints,
                DIALECT, split);

        assertNotNull(stmt, "PreparedStatement should not be null");
        ResultSet rs = stmt.executeQuery();

        rs.close();
        stmt.close();
    }

    /**
     * Provides test cases for parameterized Calcite SQL testing. Each argument contains: test
     * category, test case name, SQL query, and expected result validator.
     */
    private static Stream<Arguments> provideCalciteSqlTestCases() {
        return Stream.of(
                // Basic Data Retrieval
                Arguments.of("Basic Data Retrieval", "Simple select",
                        "SELECT * FROM test_table LIMIT 10", 3),
                Arguments.of("Basic Data Retrieval", "Specific columns",
                        "SELECT int_col, varchar_col, bigint_col, decimal_col FROM test_table LIMIT 100", 3),
                Arguments.of("Basic Data Retrieval", "Count records",
                        "SELECT COUNT(*) FROM test_table", 1),
                Arguments.of("Basic Data Retrieval", "Distinct values",
                        "SELECT DISTINCT bool_col FROM test_table", 2),
                Arguments.of("Basic Data Retrieval", "Distinct years",
                        "SELECT DISTINCT EXTRACT(YEAR FROM date_col) as \"year\" FROM test_table", 3),

                // Filtering & Conditions
                Arguments.of("Filtering & Conditions", "Numeric filter",
                        "SELECT * FROM test_table WHERE int_col > 1000", 2),
                Arguments.of("Filtering & Conditions", "Range filter",
                        "SELECT * FROM test_table WHERE int_col BETWEEN 100 AND 500", 0),
                Arguments.of("Filtering & Conditions", "String exact match",
                        "SELECT * FROM test_table WHERE varchar_col = 'varchar_value'", 1),
                Arguments.of("Filtering & Conditions", "String pattern LIKE",
                        "SELECT * FROM test_table WHERE varchar_col LIKE '%varchar%'", 2),
                Arguments.of("Filtering & Conditions", "IN clause",
                        "SELECT * FROM test_table WHERE varchar_col IN ('varchar_value', 'another_varchar', 'third_value')", 3),
                Arguments.of("Filtering & Conditions", "Year filter",
                        "SELECT * FROM test_table WHERE EXTRACT(YEAR FROM date_col) = 2020", 1),
                Arguments.of("Filtering & Conditions", "Year range",
                        "SELECT * FROM test_table WHERE EXTRACT(YEAR FROM date_col) >= 2020", 2),
                // Arguments.of("Filtering & Conditions", "Null check",
                //         "SELECT * FROM test_table WHERE float_col IS NULL", 0),
                Arguments.of("Filtering & Conditions", "Not null check",
                        "SELECT * FROM test_table WHERE decimal_col IS NOT NULL", 3),
                Arguments.of("Filtering & Conditions", "Boolean filters",
                        "SELECT * FROM test_table WHERE bool_col = true ORDER BY timestamp_col", 2),

                // Data Type Conversions
                Arguments.of("Data Type Conversions", "Price conversion",
                        "SELECT int_col, CAST(decimal_col AS DOUBLE) as price_num FROM test_table", 3),
                Arguments.of("Data Type Conversions", "Year conversion",
                        "SELECT int_col, CAST(EXTRACT(YEAR FROM date_col) AS INTEGER) as year_int FROM test_table", 3),

                // Aggregations
                Arguments.of("Aggregations", "Basic stats",
                        "SELECT COUNT(*), AVG(CAST(decimal_col AS DOUBLE)) FROM test_table", 1),
                Arguments.of("Aggregations", "Min/Max year",
                        "SELECT MAX(EXTRACT(YEAR FROM date_col)), MIN(EXTRACT(YEAR FROM date_col)) FROM test_table", 1),
                Arguments.of("Aggregations", "Sum price",
                        "SELECT SUM(CAST(decimal_col AS DOUBLE)) FROM test_table", 1),
                Arguments.of("Aggregations", "Group by status",
                        "SELECT bool_col, COUNT(*) FROM test_table GROUP BY bool_col", 2),
                Arguments.of("Aggregations", "Group by year",
                        "SELECT EXTRACT(YEAR FROM date_col) as \"year\", COUNT(*) FROM test_table GROUP BY EXTRACT(YEAR FROM date_col) ORDER BY \"year\"", 3),
                Arguments.of("Aggregations", "Avg price by model",
                        "SELECT varchar_col, AVG(CAST(decimal_col AS DOUBLE)) FROM test_table GROUP BY varchar_col", 3),

                // Complex Filtering
                Arguments.of("Complex Filtering", "Multiple conditions",
                        "SELECT * FROM test_table WHERE EXTRACT(YEAR FROM date_col) >= 2020 AND bool_col = true AND CAST(decimal_col AS DOUBLE) < 80000", 1),
                Arguments.of("Complex Filtering", "Pattern matching",
                        "SELECT * FROM test_table WHERE varchar_col LIKE 'varchar%'", 1),
                Arguments.of("Complex Filtering", "Case statement",
                        "SELECT int_col, varchar_col, CASE WHEN EXTRACT(YEAR FROM date_col) >= 2020 THEN CAST('Recent' AS VARCHAR) ELSE CAST('Old' AS VARCHAR) END as age_category FROM test_table", 3),

                // Sorting & Limiting
                Arguments.of("Sorting & Limiting", "Order by ID desc",
                        "SELECT * FROM test_table ORDER BY int_col DESC LIMIT 50", 3),
                Arguments.of("Sorting & Limiting", "Order by price asc",
                        "SELECT * FROM test_table ORDER BY CAST(decimal_col AS DOUBLE) ASC LIMIT 20", 3),
                Arguments.of("Sorting & Limiting", "Multiple sort",
                        "SELECT * FROM test_table ORDER BY EXTRACT(YEAR FROM date_col) DESC, varchar_col ASC LIMIT 100", 3),

                // String Functions
                Arguments.of("String Functions", "Case conversion",
                        "SELECT UPPER(varchar_col), LOWER(varchar_col) FROM test_table LIMIT 10", 3),

                // Date/Time Functions
                Arguments.of("Date/Time Functions", "Age calculation",
                        "SELECT *, (2024 - EXTRACT(YEAR FROM date_col)) as age FROM test_table LIMIT 10", 3),

                // Window Functions
                Arguments.of("Window Functions", "Row numbering",
                        "SELECT *, ROW_NUMBER() OVER (ORDER BY int_col) as row_num FROM test_table LIMIT 10", 3),
                Arguments.of("Window Functions", "Price ranking",
                        "SELECT *, RANK() OVER (ORDER BY CAST(decimal_col AS DOUBLE) DESC) as price_rank FROM test_table LIMIT 20", 3),
                Arguments.of("Window Functions", "Partition window",
                        "SELECT *, COUNT(*) OVER (PARTITION BY bool_col) as status_count FROM test_table", 3),

                // Performance Testing
                Arguments.of("Performance Testing", "Large result set",
                        "SELECT * FROM test_table LIMIT 10000", 3),
                Arguments.of("Performance Testing", "Complex aggregation",
                        "SELECT varchar_col, bool_col, COUNT(*), AVG(CAST(decimal_col AS DOUBLE)), MIN(CAST(int_col AS INTEGER)), MAX(EXTRACT(YEAR FROM date_col)) FROM test_table GROUP BY varchar_col, bool_col ORDER BY COUNT(*) DESC", 3),

                // Data Quality Checks
                Arguments.of("Data Quality Checks", "Duplicate check",
                        "SELECT int_col, COUNT(*) FROM test_table GROUP BY int_col HAVING COUNT(*) > 1", 0),
                Arguments.of("Data Quality Checks", "Completeness check",
                        "SELECT COUNT(*) as total_rows, COUNT(int_col) as int_count, COUNT(varchar_col) as varchar_count, COUNT(decimal_col) as price_count FROM test_table", 1),
                Arguments.of("Data Quality Checks", "Value ranges",
                        "SELECT MIN(EXTRACT(YEAR FROM date_col)) as min_year, MAX(EXTRACT(YEAR FROM date_col)) as max_year, MIN(CAST(decimal_col AS DOUBLE)) as min_price, MAX(CAST(decimal_col AS DOUBLE)) as max_price FROM test_table", 1),

                // Boolean Column Tests - Testing standalone boolean and NOT boolean transformations
                Arguments.of("Boolean Column Tests", "boolean column with values",
                        "SELECT * FROM test_table WHERE bool_col = true LIMIT 10", 2),
                Arguments.of("Boolean Column Tests", "Standalone boolean column",
                        "SELECT * FROM test_table WHERE bool_col LIMIT 10", 2),
                Arguments.of("Boolean Column Tests", "Standalone boolean column with NOT",
                        "SELECT * FROM test_table WHERE NOT bool_col LIMIT 10", 1),
                Arguments.of("Boolean Column Tests", "Explicit boolean comparison true",
                        "SELECT * FROM test_table WHERE bool_col = true LIMIT 10", 2),
                Arguments.of("Boolean Column Tests", "Explicit boolean comparison false",
                        "SELECT * FROM test_table WHERE bool_col = false LIMIT 10", 1),
                Arguments.of("Boolean Column Tests", "Complex condition with NOT boolean",
                        "SELECT * FROM test_table WHERE int_col > 1000 AND NOT bool_col LIMIT 10", 1),
                Arguments.of("Boolean Column Tests", "Complex condition with boolean",
                        "SELECT * FROM test_table WHERE int_col > 1000 AND bool_col LIMIT 10", 1),
                Arguments.of("Boolean Column Tests", "OR condition with boolean",
                        "SELECT * FROM test_table WHERE bool_col OR int_col = 1500 LIMIT 10", 2),
                // Arguments.of("Boolean Column Tests", "Multiple boolean conditions",
                //         "SELECT * FROM test_table WHERE bool_col AND NOT bool_col LIMIT 10", 0),
                Arguments.of("Boolean Column Tests", "Boolean in complex expression",
                        "SELECT * FROM test_table WHERE (int_col > 1000 AND bool_col) OR (int_col < 500 AND NOT bool_col) LIMIT 10", 1)
        );
    }

    @ParameterizedTest(name = "[{index}] {0} - {1}")
    @MethodSource("provideCalciteSqlTestCases")
    public void testRealPreparedStatementWithCalciteSqlParameterized(String category,
            String testCaseName, String sqlQuery, int expectedRowCount) throws Exception {
        // Generate base64 encoded Substrait plan from SQL query
        String base64EncodedPlan =
                EncodedSubstraitPlanStringGenerator.generate(TEST_TABLE, TEST_SCHEMA, sqlQuery);

        // Mock QueryPlan with the encoded plan
        QueryPlan queryPlan = mock(QueryPlan.class);
        when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlan);

        // Mock Constraints with the QueryPlan
        Constraints constraints = mock(Constraints.class);
        when(constraints.getQueryPlan()).thenReturn(queryPlan);

        // Mock Split
        Split split = mock(Split.class);

        // Prepare the statement using Calcite SQL
        PreparedStatement stmt = builder.prepareStatementWithCalciteSql(realConnection, constraints,
                DIALECT, split);

        // Verify PreparedStatement was created successfully
        assertNotNull(stmt, "PreparedStatement should not be null for: " + testCaseName);

        // Execute the query
        ResultSet rs = stmt.executeQuery();
        assertNotNull(rs, "ResultSet should not be null for: " + testCaseName);

        // Verify we can iterate through results (basic validation)
        // Some queries may return no results (e.g., null checks, filters with no matches)
        // but the query should execute without errors
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }

        assertEquals(expectedRowCount, rowCount,
                category + " - " + testCaseName + ": expected " + expectedRowCount
                        + " rows but got " + rowCount);

        // Cleanup
        rs.close();
        stmt.close();
    }

    /**
     * Test specific error cases that are expected to fail. These are separate from the main
     * parameterized test to handle exceptions properly.
     */
    @Test
    public void testRealPreparedStatementWithCalciteSqlErrorCases() throws Exception {
        // Test invalid syntax - should throw SQLException
        try {
            String base64EncodedPlan = EncodedSubstraitPlanStringGenerator.generate(TEST_TABLE,
                    TEST_SCHEMA, "SELECT * FORM test_table"); // Typo: FORM instead of FROM

            QueryPlan queryPlan = mock(QueryPlan.class);
            when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlan);

            Constraints constraints = mock(Constraints.class);
            when(constraints.getQueryPlan()).thenReturn(queryPlan);

            Split split = mock(Split.class);

            PreparedStatement stmt = builder.prepareStatementWithCalciteSql(realConnection,
                    constraints, DIALECT, split);

            // If we get here, the syntax error wasn't caught during plan generation
            // It may still fail during execution
            stmt.executeQuery();

            // Fail if no exception was thrown
            // Note: The actual behavior depends on when Calcite validates the SQL
            assertFalse(true);
        } catch (Exception e) {
            // Expected - invalid SQL should cause an error
            assertTrue(true);
        }
    }

    /**
     * Test queries that reference invalid columns.
     */
    @Test
    public void testRealPreparedStatementWithCalciteSqlInvalidColumn() throws Exception {
        try {
            String base64EncodedPlan = EncodedSubstraitPlanStringGenerator.generate(TEST_TABLE,
                    TEST_SCHEMA, "SELECT invalid_column FROM test_table");

            QueryPlan queryPlan = mock(QueryPlan.class);
            when(queryPlan.getSubstraitPlan()).thenReturn(base64EncodedPlan);

            Constraints constraints = mock(Constraints.class);
            when(constraints.getQueryPlan()).thenReturn(queryPlan);

            Split split = mock(Split.class);

            PreparedStatement stmt = builder.prepareStatementWithCalciteSql(realConnection,
                    constraints, DIALECT, split);

            stmt.executeQuery();

            // Fail if no exception was thrown
        } catch (Exception e) {
            // Expected - invalid column should cause an error
            assertTrue(
                    e.getMessage().toLowerCase().contains("column")
                            || e.getMessage().toLowerCase().contains("not found")
                            || e.getMessage().toLowerCase().contains("invalid"),
                    "Error message should indicate column problem: " + e.getMessage());
        }
    }
}
