/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

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
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Generates base64-encoded Substrait plan strings from SQL queries
 * for the GCS test table schema (id, first_name, last_name, email,
 * gender, ip_address - all VARCHAR).
 */
public class GcsSubstraitPlanGenerator
{
    public static final String TABLE_NAME = "test_gcs_table";
    public static final String SCHEMA_NAME = "test_gcs_database";

    private GcsSubstraitPlanGenerator()
    {
    }

    /**
     * Generates a base64-encoded Substrait plan from a SQL query.
     *
     * @param sql SQL query referencing columns: id, first_name, last_name, email, gender, ip_address
     * @return base64-encoded Substrait plan string
     */
    public static String generate(String sql) throws Exception
    {
        Table table = createGcsTestTable();
        Schema schema = new SubstraitSchema(Map.of(TABLE_NAME, table));
        Plan plan = convertSqlToSubstraitPlan(SCHEMA_NAME, schema, sql);
        return Base64.getEncoder().encodeToString(plan.toByteArray());
    }

    private static Plan convertSqlToSubstraitPlan(String schemaName, Schema schema, String sql)
            throws Exception
    {
        PlanProtoConverter planProtoConverter = new PlanProtoConverter();
        SqlToSubstrait sqlToSubstrait = new SqlToSubstrait();
        CalciteCatalogReader catalog = schemaToCatalog(schemaName, schema);
        return planProtoConverter.toProto(sqlToSubstrait.convert(sql, catalog));
    }

    private static CalciteCatalogReader schemaToCatalog(String schemaName, Schema schema)
    {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        rootSchema.add(schemaName, schema);
        List<String> defaultSchema = List.of(schemaName);
        return new CalciteCatalogReader(
                rootSchema,
                defaultSchema,
                new SqlTypeFactoryImpl(AnsiSqlDialect.DEFAULT.getTypeSystem()),
                CalciteConnectionConfig.DEFAULT
                        .set(CalciteConnectionProperty.CASE_SENSITIVE, Boolean.FALSE.toString()));
    }

    /**
     * Creates a Calcite table definition matching the GCS test CSV data:
     * id, first_name, last_name, email, gender, ip_address (all VARCHAR).
     */
    private static Table createGcsTestTable()
    {
        return new AbstractTable()
        {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory)
            {
                return factory.createStructType(Arrays.asList(
                        Pair.of("id", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("first_name", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("last_name", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("email", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("gender", factory.createSqlType(SqlTypeName.VARCHAR)),
                        Pair.of("ip_address", factory.createSqlType(SqlTypeName.VARCHAR))));
            }
        };
    }
}
