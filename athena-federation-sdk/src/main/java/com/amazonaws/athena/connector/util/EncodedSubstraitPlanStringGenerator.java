/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.util;

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
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class EncodedSubstraitPlanStringGenerator 
{
    private EncodedSubstraitPlanStringGenerator() 
    {
        
    }
    public static final String CALCITE_TEST_TABLE_NAME = "test_table";
    public static final String CALCITE_TEST_SCHEMA_NAME = "test_schema";
    public static final SqlDialect DIALECT = AnsiSqlDialect.DEFAULT;

    public static String generate(final String inputTable, final String inputSchema,
            final String inputSql) throws SqlParseException 
    {
        final Table table = EncodedSubstraitPlanStringGenerator.createTable();
        final Schema schema = new SubstraitSchema(Map.of(inputTable, table));
        final Plan plan = EncodedSubstraitPlanStringGenerator.convertSqlToSubstraitPlan(inputSchema,
                schema, inputSql);
        final String encodedPlan = Base64.getEncoder().encodeToString(plan.toByteArray());

        return encodedPlan;
    }

    public static Plan convertSqlToSubstraitPlan(final String query) throws SqlParseException 
    {
        final Schema schema = new SubstraitSchema(Map.of(CALCITE_TEST_TABLE_NAME, createTable()));
        return convertSqlToSubstraitPlan(CALCITE_TEST_SCHEMA_NAME, schema, query);
    }

    public static Plan convertSqlToSubstraitPlan(final String inputSchema, final Schema schema,
            final String inputSql) throws SqlParseException         
    {
        final PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        final SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        final CalciteCatalogReader catalog = schemaToCatalog(inputSchema, schema);
        return planProtoConverter.toProto(sqlToSubstrait.convert(inputSql, catalog));
    }

    public static CalciteCatalogReader schemaToCatalog(final String schemaName,
            final Schema schema) 
    {
        final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(schemaName, schema);
        final List<String> defaultSchema = List.of(schemaName);
        return new CalciteCatalogReader(rootSchema, defaultSchema,
                new SqlTypeFactoryImpl(DIALECT.getTypeSystem()), CalciteConnectionConfig.DEFAULT
                        .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
    }

    public static Table createTable() 
    {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(final RelDataTypeFactory factory) 
            {
                return factory.createStructType(Arrays.asList(
                        // Boolean types
                        Pair.of("bool_col", factory.createSqlType(SqlTypeName.BOOLEAN)),

                        // Integer types (I8, I16, I32, I64)
                        Pair.of("tinyint_col", factory.createSqlType(SqlTypeName.TINYINT)),
                        Pair.of("smallint_col", factory.createSqlType(SqlTypeName.SMALLINT)),
                        Pair.of("int_col", factory.createSqlType(SqlTypeName.INTEGER)),
                        Pair.of("bigint_col", factory.createSqlType(SqlTypeName.BIGINT)),

                        // Floating point types (Fp32, Fp64)
                        Pair.of("float_col", factory.createSqlType(SqlTypeName.FLOAT)),
                        Pair.of("double_col", factory.createSqlType(SqlTypeName.DOUBLE)),
                        Pair.of("real_col", factory.createSqlType(SqlTypeName.REAL)),

                        // String and binary types
                        Pair.of("varchar_col", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("char_col", factory.createSqlType(SqlTypeName.CHAR)),
                        Pair.of("binary_col", factory.createSqlType(SqlTypeName.BINARY)),
                        Pair.of("varbinary_col", factory.createSqlType(SqlTypeName.VARBINARY)),

                        // Temporal types (Date, Time, Timestamp)
                        Pair.of("date_col", factory.createSqlType(SqlTypeName.DATE)),
                        Pair.of("time_col", factory.createSqlType(SqlTypeName.TIME)),
                        Pair.of("timestamp_col", factory.createSqlType(SqlTypeName.TIMESTAMP)),

                        // Decimal types with various precision and scale
                        Pair.of("decimal_col", factory.createSqlType(SqlTypeName.DECIMAL, 19, 4)),
                        Pair.of("decimal_col2", factory.createSqlType(SqlTypeName.DECIMAL, 10, 2)),
                        Pair.of("decimal_col3", factory.createSqlType(SqlTypeName.DECIMAL, 38, 10)),

                        // Array/List types
                        Pair.of("array_col",
                                factory.createArrayType(factory.createSqlType(SqlTypeName.VARCHAR),
                                        -1)),
                        Pair.of("int_array_col",
                                factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER),
                                        -1)),

                        // Map types
                        Pair.of("map_col",
                                factory.createMapType(factory.createSqlType(SqlTypeName.VARCHAR),
                                        factory.createSqlType(SqlTypeName.INTEGER))),

                        // Complex nested types
                        Pair.of("map_with_decimal",
                                factory.createMapType(factory.createSqlType(SqlTypeName.VARCHAR),
                                        factory.createSqlType(SqlTypeName.DECIMAL, 10, 2))),
                        Pair.of("nested_array",
                                factory.createArrayType(
                                        factory.createArrayType(
                                                factory.createSqlType(SqlTypeName.INTEGER), -1),
                                        -1))));
            }
        };
    }
}
