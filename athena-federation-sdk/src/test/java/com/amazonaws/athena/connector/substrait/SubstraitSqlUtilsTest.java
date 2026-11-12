/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
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
package com.amazonaws.athena.connector.substrait;

import com.amazonaws.athena.connector.util.EncodedSubstraitPlanStringGenerator;
import io.substrait.isthmus.calcite.SubstraitSchema;
import io.substrait.proto.Plan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import java.util.Base64;
import java.util.List;
import java.util.Map;

public class SubstraitSqlUtilsTest {
    @Test
    void testGetTableSchemaFromSubstraitPlan_AllArrowTypes() throws SqlParseException {
        // Test schema extraction with all supported types including complex types
        String query = "SELECT * FROM test_table LIMIT 1";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);

        RelDataType schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.getFieldCount() > 0);

        // Verify comprehensive schema includes all types
        List<String> fieldNames = schema.getFieldNames();

        // Check for presence of various type categories
        Assertions.assertTrue(fieldNames.contains("bool_col"), "Missing boolean type");
        Assertions.assertTrue(fieldNames.contains("array_col"), "Missing array type");
        Assertions.assertTrue(fieldNames.contains("map_col"), "Missing map type");
        // Assertions.assertTrue(fieldNames.contains("struct_col"), "Missing struct type");
    }

    @Test
    void testGetTableSchemaFromSubstraitPlan_BasicTypes() throws SqlParseException {
        // Test with simple schema
        String query = "SELECT * FROM test_table";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);

        RelDataType schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(schema);
        Assertions.assertFalse(schema.getFieldList().isEmpty());

        // Verify field names match the test table schema
        List<String> fieldNames = schema.getFieldNames();
        Assertions.assertTrue(fieldNames.contains("varchar_col"));
        Assertions.assertTrue(fieldNames.contains("int_col"));
    }

    @Test
    void testGetTableSchemaFromSubstraitPlan_WithWhereClause() throws SqlParseException {
        String query = "SELECT * FROM test_table WHERE varchar_col = 'test'";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);

        RelDataType schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.getFieldCount() > 0);
    }

    @Test
    void testGetTableSchemaFromSubstraitPlan_ComplexQuery() throws SqlParseException {
        String query =
                "SELECT * FROM test_table WHERE varchar_col LIKE 'A%' ORDER BY int_col DESC LIMIT 10";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);

        RelDataType schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.getFieldCount() > 0);
    }

    @Test
    void testGetSqlNodeFromSubstraitPlan_InvalidBase64() {
        String invalidBase64 = "This is not valid base64!!!@#$";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(invalidBase64,
                    EncodedSubstraitPlanStringGenerator.DIALECT);
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to parse Substrait plan"));
    }

    @Test
    void testGetSqlNodeFromSubstraitPlan_EmptyString() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SubstraitSqlUtils.getSqlNodeFromSubstraitPlan("",
                    EncodedSubstraitPlanStringGenerator.DIALECT);
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to parse Substrait plan"));
    }

    @Test
    void testGetSqlNodeFromSubstraitPlan_InvalidProtobuf() {
        // Valid base64 but invalid protobuf
        String invalidProtobuf =
                Base64.getEncoder().encodeToString("invalid protobuf data".getBytes());

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(invalidProtobuf,
                    EncodedSubstraitPlanStringGenerator.DIALECT);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("Failed to parse Substrait plan") || exception
                        .getMessage().contains("Failed to convert from Substrait plan to RelNode"));
    }

    @Test
    void testGetTableSchemaFromSubstraitPlan_InvalidBase64() {
        String invalidBase64 = "Not valid base64 content!!!";

        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(invalidBase64,
                    EncodedSubstraitPlanStringGenerator.DIALECT);
        });

        Assertions.assertTrue(exception.getMessage()
                .contains("Failed to extract table schema from Substrait plan"));
    }

    @Test
    void testGetTableSchemaFromSubstraitPlan_EmptyPlan() {
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            SubstraitSqlUtils.getTableSchemaFromSubstraitPlan("",
                    EncodedSubstraitPlanStringGenerator.DIALECT);
        });

        Assertions.assertTrue(exception.getMessage()
                .contains("Failed to extract table schema from Substrait plan"));
    }

    @Test
    void testSqlConversion_IsNotNull() throws SqlParseException {
        String query = "SELECT * FROM test_table WHERE varchar_col IS NOT NULL";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);
        String sqlStr = sql.toSqlString(EncodedSubstraitPlanStringGenerator.DIALECT).getSql();

        Assertions.assertNotNull(sqlStr);
    }

    @Test
    void testSqlConversion_Between() throws SqlParseException {
        String query = "SELECT * FROM test_table WHERE int_col BETWEEN 10 AND 100";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(sql);
    }

    @Test
    void testSqlConversion_CaseExpression() throws SqlParseException {
        String query =
                "SELECT CASE WHEN int_col > 100 THEN CAST('high' AS VARCHAR) ELSE CAST('low' AS VARCHAR) END FROM test_table";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(sql);
    }

    @Test
    void testSqlConversion_MultipleAggregations() throws SqlParseException {
        String query = "SELECT varchar_col, COUNT(*), SUM(int_col), AVG(int_col) "
                + "FROM test_table GROUP BY varchar_col";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(sql);
    }

    @Test
    void testSqlConversion_NotIn() throws SqlParseException {
        String query = "SELECT * FROM test_table WHERE varchar_col NOT IN ('val1', 'val2')";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);
        String sqlStr = sql.toSqlString(EncodedSubstraitPlanStringGenerator.DIALECT).getSql();

        Assertions.assertTrue(sqlStr.contains("NOT IN") || sqlStr.contains("<>"));
    }

    @Test
    void testSqlConversion_Having() throws SqlParseException {
        String query =
                "SELECT varchar_col, COUNT(*) FROM test_table GROUP BY varchar_col HAVING COUNT(*) > 5";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(sql);
    }

    @Test
    void testSqlConversion_Distinct() throws SqlParseException {
        String query = "SELECT DISTINCT varchar_col FROM test_table";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);
        String sqlStr = sql.toSqlString(EncodedSubstraitPlanStringGenerator.DIALECT).getSql();

        Assertions.assertTrue(sqlStr.toUpperCase().contains("DISTINCT")
                || sqlStr.toUpperCase().contains("GROUP BY"));
    }

    @Test
    void testSqlConversion_ComplexArithmetic() throws SqlParseException {
        String query = "SELECT (int_col * 2 + 10) / 3 AS calculated_result FROM test_table";
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(query);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);

        Assertions.assertNotNull(sql);
    }

    @ParameterizedTest
    @MethodSource("provideSqlTestCases")
    void testSqlConversion(String inputSql, String expectedSql) throws SqlParseException {
        Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(inputSql);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan,
                EncodedSubstraitPlanStringGenerator.DIALECT);
        String sqlStr = sql.toSqlString(EncodedSubstraitPlanStringGenerator.DIALECT).getSql();
        RelDataType tableSchema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(encodedPlan, EncodedSubstraitPlanStringGenerator.DIALECT);
        Assertions.assertEquals(expectedSql, sqlStr);
        Assertions.assertNotNull(tableSchema);
    }

    // HELPER METHODS
    // ================================================================================================================
    @Test
    void testSubstraitPlanGeneration() throws SqlParseException {
        // Add your test inputs for <inputTable, inputSchema, inputSql>
        final String inputTable = "INPUT_TABLE";
        final Table table = EncodedSubstraitPlanStringGenerator.createTable();
        final String inputSchema = "INPUT_SCHEMA";
        final Schema schema = new SubstraitSchema(Map.of(inputTable, table));
        final String inputSql = "SELECT * FROM INPUT_TABLE LIMIT 10;";

        // No need to modify below
        final Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(inputSchema,
                schema, inputSql);
        final String encodedPlan = Base64.getEncoder().encodeToString(plan.toByteArray());

        // encodedPlan is what connector lambda expects from Trino
        // visualization for encodedPlan
        final Plan visualizedPlan = SubstraitRelUtils.deserializeSubstraitPlan(encodedPlan);
        Assertions.assertNotNull(visualizedPlan);
    }

    private static Stream<Arguments> provideSqlTestCases() {
        return Stream.of(
                // Basic logical operators
                Arguments.of(
                        "SELECT * FROM test_table WHERE varchar_col = 'value1' AND int_col > 100",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` = 'value1' AND `int_col` > 100"),
                Arguments.of(
                        "SELECT * FROM test_table WHERE varchar_col = 'value1' OR int_col < 50",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` = 'value1' OR `int_col` < 50"),
                Arguments.of("SELECT * FROM test_table WHERE NOT (varchar_col = 'excluded_value')",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` <> 'excluded_value'"),

                // Comparison operators
                Arguments.of("SELECT * FROM test_table WHERE varchar_col != 'excluded_value'",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` <> 'excluded_value'"),
                Arguments.of("SELECT * FROM test_table WHERE int_col < 100",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `int_col` < 100"),
                Arguments.of("SELECT * FROM test_table WHERE int_col >= 50",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `int_col` >= 50"),

                // Arithmetic operations
                Arguments.of(
                        "SELECT varchar_col, int_col + 10 as added_value FROM test_table WHERE int_col + 5 > 100",
                        "SELECT `varchar_col` AS `varchar_col0`, `int_col` + 10 AS `$f24`\n"
                                + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `int_col` + 5 > 100"),
                Arguments.of("SELECT * FROM test_table WHERE int_col % 2 = 0",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE MOD(`int_col`, 2) = 0"),

                // Pattern matching
                Arguments.of("SELECT * FROM test_table WHERE varchar_col LIKE 'prefix%'",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` LIKE 'prefix%'"),

                // IN operations
                Arguments.of(
                        "SELECT * FROM test_table WHERE varchar_col IN ('value1', 'value2', 'value3')",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` IN ('value1', 'value2', 'value3')"),
                Arguments.of("SELECT * FROM test_table WHERE int_col IN (10, 20, 30)",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `int_col` IN (10, 20, 30)"),

                // LIMIT operations
                Arguments.of("SELECT * FROM test_table LIMIT 10",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "FETCH NEXT 10 ROWS ONLY"),

                // ORDER BY operations
                Arguments.of("SELECT * FROM test_table ORDER BY varchar_col ASC",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "ORDER BY `varchar_col`"),
                Arguments.of("SELECT * FROM test_table ORDER BY varchar_col DESC",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "ORDER BY `varchar_col` DESC"),
                Arguments.of("SELECT * FROM test_table ORDER BY varchar_col ASC, int_col DESC",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "ORDER BY `varchar_col`, `int_col` DESC"),

                // Combined operations
                Arguments.of(
                        "SELECT * FROM test_table WHERE varchar_col LIKE 'A%' AND int_col > 100 ORDER BY varchar_col LIMIT 25",
                        "SELECT *\n" + "FROM `test_schema`.`test_table`\n"
                                + "WHERE `varchar_col` LIKE 'A%' AND `int_col` > 100\n"
                                + "ORDER BY `varchar_col`\n" + "FETCH NEXT 25 ROWS ONLY"),
                // GROUP BY operations
                Arguments.of(
                        "SELECT varchar_col, COUNT(*) FROM test_table WHERE varchar_col IS NOT NULL GROUP BY varchar_col ORDER BY COUNT(*) DESC LIMIT 50",
                        "SELECT `varchar_col` AS `varchar_col0`, COUNT(*) AS `$f1`\n"
                                + "FROM `test_schema`.`test_table`\n" + "GROUP BY `varchar_col`\n"
                                + "ORDER BY 2 DESC\n" + "FETCH NEXT 50 ROWS ONLY"));
    }
}
