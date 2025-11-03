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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                final TypeHelper helper = new TypeHelper(factory);
                return factory.createStructType(Arrays.asList(Pair.of("column1", helper.string()),
                        Pair.of("column2", helper.i32()), Pair.of("column3", helper.string()),
                        Pair.of("numeric_column", helper.i32()),
                        Pair.of("date_column", helper.date()),
                        Pair.of("string_column", helper.string()),
                        Pair.of("array_column", helper.list(helper.string())),
                        Pair.of("id", helper.i32()), Pair.of("foreign_id", helper.i32()),
                        Pair.of("status", helper.string()),
                        Pair.of("created_date", helper.date())));
            }
        };

        final Schema schema = new SubstraitSchema(Map.of(CALCITE_TEST_TABLE_NAME, table));
        return schema;
    }

    private class TypeHelper {
        private final RelDataTypeFactory factory;

        public TypeHelper(final RelDataTypeFactory factory) {
            this.factory = factory;
        }

        RelDataType struct(final String field, final RelDataType value) {
            return factory.createStructType(Arrays.asList(Pair.of(field, value)));
        }

        RelDataType struct2(final String field1, final RelDataType value1, final String field2,
                final RelDataType value2) {
            return factory.createStructType(
                    Arrays.asList(Pair.of(field1, value1), Pair.of(field2, value2)));
        }

        RelDataType i32() {
            return factory.createSqlType(SqlTypeName.INTEGER);
        }

        RelDataType string() {
            return factory.createSqlType(SqlTypeName.VARCHAR);
        }

        RelDataType date() {
            return factory.createSqlType(SqlTypeName.DATE);
        }

        RelDataType list(final RelDataType elementType) {
            return factory.createArrayType(elementType, -1);
        }

        RelDataType map(final RelDataType key, final RelDataType value) {
            return factory.createMapType(key, value);
        }
    }
}
