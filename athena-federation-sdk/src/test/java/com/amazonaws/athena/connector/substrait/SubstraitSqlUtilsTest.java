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

import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.calcite.SubstraitSchema;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.Plan;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class SubstraitSqlUtilsTest {

    private static final String CALCITE_TEST_TABLE_NAME = "test_table";
    private static final String CALCITE_TEST_SCHEMA_NAME = "test_schema";
    private static final SqlDialect DIALECT = AnsiSqlDialect.DEFAULT;

    @ParameterizedTest
    @MethodSource("provideSqlTestCases")
    void testSqlConversion(String inputSql, String expectedSql) throws SqlParseException {
        Plan plan = convertSqlToSubstraitPlan(inputSql);
        byte[] planBytes = plan.toByteArray();
        String encodedPlan = Base64.getEncoder().encodeToString(planBytes);
        SqlNode sql = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(encodedPlan, DIALECT);
        String sqlStr = sql.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql();
        Assertions.assertEquals(expectedSql, sqlStr);
    }

    @Test
    void generatePlan() throws SqlParseException {
        // Add your test inputs for <inputTable, inputSql, inputSchema, inputGetRowType>
        final String inputTable = "ORACLE_BASIC_DBTABLE_GAMMA_EU_WEST_1_INTEG";
        final String inputSql = "SELECT * FROM \"ORACLE_BASIC_DBTABLE_GAMMA_EU_WEST_1_INTEG\" LIMIT 10;";
        final String inputSchema = "ORACLE_BASIC_DBTABLE_SCHEMA_GAMMA_EU_WEST_1_INTEG";
        final Function<RelDataTypeFactory, RelDataType> inputGetRowType = (factory) -> factory.createStructType(Arrays.asList(
                Pair.of("employee_id", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BINARY), true)),
                Pair.of("is_active", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("employee_name", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true)),
                Pair.of("job_title", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true)),
                Pair.of("address", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true)),
                Pair.of("join_date", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DATE), true)),
                Pair.of("timestamp", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.TIMESTAMP), true)),
                Pair.of("duration", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true)),
                Pair.of("salary", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DECIMAL, 19, 4), true)),
                Pair.of("bonus", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DECIMAL, 19, 4), true)),
                Pair.of("hash1", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("hash2", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("code", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("debit", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DECIMAL, 19, 3), true)),
                Pair.of("count", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("amount", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DECIMAL, 19, 3), true)),
                Pair.of("balance", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("rate", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DECIMAL, 19, 3), true)),
                Pair.of("difference", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true)),
                Pair.of("partition_name", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), true))));

        // No need to modify below
        final PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        final Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory factory) {
                return inputGetRowType.apply(factory);
            }
        };
        final Schema schema = new SubstraitSchema(Map.of(inputTable, table));
        final CalciteCatalogReader catalog = schemaToCatalog(inputSchema, schema);
        final Plan plan = planProtoConverter.toProto(sqlToSubstrait.convert(inputSql, catalog));
        final byte[] planBytes = plan.toByteArray();
        final String encodedPlan = Base64.getEncoder().encodeToString(planBytes);

        System.out.println(encodedPlan);
        System.out.println(SubstraitRelUtils.deserializeSubstraitPlan(encodedPlan));

        Assertions.assertNotNull(encodedPlan);
    }


    private static Stream<Arguments> provideSqlTestCases() {
        return Stream.of(
                // Basic logical operators
                Arguments.of("SELECT * FROM test_table WHERE column1 = 'value1' AND column2 > 100",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" = 'value1' AND \"column2\" > 100"),
                Arguments.of("SELECT * FROM test_table WHERE column1 = 'value1' OR column2 < 50",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" = 'value1' OR \"column2\" < 50"),
                Arguments.of("SELECT * FROM test_table WHERE NOT (column1 = 'excluded_value')",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" <> 'excluded_value'"),

                // Comparison operators
                Arguments.of("SELECT * FROM test_table WHERE column1 != 'excluded_value'",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" <> 'excluded_value'"),
                Arguments.of("SELECT * FROM test_table WHERE numeric_column < 100",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"numeric_column\" < 100"),
                Arguments.of("SELECT * FROM test_table WHERE numeric_column >= 50",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"numeric_column\" >= 50"),

                // Arithmetic operations
                Arguments.of(
                        "SELECT column1, numeric_column + 10 as added_value FROM test_table WHERE numeric_column + 5 > 100",
                        "SELECT \"column1\" AS \"column10\", \"numeric_column\" + 10 AS \"$f12\"\n"
                                + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"numeric_column\" + 5 > 100"),
                Arguments.of("SELECT * FROM test_table WHERE numeric_column % 2 = 0",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE MOD(\"numeric_column\", 2) = 0"),

                // Pattern matching
                Arguments.of("SELECT * FROM test_table WHERE column1 LIKE 'prefix%'",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" LIKE 'prefix%'"),

                // IN operations
                Arguments.of(
                        "SELECT * FROM test_table WHERE column1 IN ('value1', 'value2', 'value3')",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" IN ('value1', 'value2', 'value3')"),
                Arguments.of("SELECT * FROM test_table WHERE numeric_column IN (10, 20, 30)",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"numeric_column\" IN (10, 20, 30)"),

                // LIMIT operations
                Arguments.of("SELECT * FROM test_table LIMIT 10",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n" + "LIMIT 10"),

                // ORDER BY operations
                Arguments.of("SELECT * FROM test_table ORDER BY column1 ASC",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "ORDER BY \"column1\""),
                Arguments.of("SELECT * FROM test_table ORDER BY column1 DESC",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "ORDER BY \"column1\" DESC"),
                Arguments.of("SELECT * FROM test_table ORDER BY column1 ASC, numeric_column DESC",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "ORDER BY \"column1\", \"numeric_column\" DESC"),

                // Combined operations
                Arguments.of(
                        "SELECT * FROM test_table WHERE column1 LIKE 'A%' AND numeric_column > 100 ORDER BY column1 LIMIT 25",
                        "SELECT *\n" + "FROM \"test_schema\".\"test_table\"\n"
                                + "WHERE \"column1\" LIKE 'A%' AND \"numeric_column\" > 100\n"
                                + "ORDER BY \"column1\"\n" + "LIMIT 25"),
                // GROUP BY operations
                Arguments.of(
                        "SELECT column1, COUNT(*) FROM test_table WHERE column1 IS NOT NULL GROUP BY column1 ORDER BY COUNT(*) DESC LIMIT 50",
                        "SELECT \"column1\" AS \"column10\", COUNT(*) AS \"$f1\"\n"
                                + "FROM \"test_schema\".\"test_table\"\n" + "GROUP BY \"column1\"\n"
                                + "ORDER BY 2 DESC\n" + "LIMIT 50"));
    }

    private Plan convertSqlToSubstraitPlan(final String query) throws SqlParseException {
        final PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        final CalciteCatalogReader catalog =
                schemaToCatalog(CALCITE_TEST_SCHEMA_NAME, createTableSchema());
        return planProtoConverter.toProto(sqlToSubstrait.convert(query, catalog));
    }

    private CalciteCatalogReader schemaToCatalog(final String schemaName, final Schema schema) {
        final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(schemaName, schema);
        final List<String> defaultSchema = List.of(schemaName);
        return new CalciteCatalogReader(rootSchema, defaultSchema,
                new SqlTypeFactoryImpl(DIALECT.getTypeSystem()), CalciteConnectionConfig.DEFAULT
                .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
    }

    private Schema createTableSchema() {
        final Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory factory) {
                return factory.createStructType(Arrays.asList(
                        Pair.of("column1", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("column2", factory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("column3", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("numeric_column", factory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("date_column", factory.createSqlType(SqlTypeName.DATE)),
                        Pair.of("string_column", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("array_column", factory.createArrayType(factory.createSqlType(SqlTypeName.VARCHAR), -1)),
                        Pair.of("id", factory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("foreign_id", factory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("status", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("created_date", factory.createSqlType(SqlTypeName.DATE))));
            }
        };

        return new SubstraitSchema(Map.of(CALCITE_TEST_TABLE_NAME, table));
    }


}
