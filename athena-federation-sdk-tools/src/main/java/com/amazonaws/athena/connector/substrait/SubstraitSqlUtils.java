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

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import io.substrait.isthmus.TypeConverter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for working with Calcite's abstract syntax tree representation of Substrait plans.
 */
public final class SubstraitSqlUtils
{
    private SubstraitSqlUtils()
    {
    }

    /**
     * Deserializes a Substrait plan with schema-aware processing.
     * Uses CustomSubstraitToCalcite to resolve table and column references.
     */
    public static SqlNode deserializeSubstraitPlan(String planString, SqlDialect sqlDialect, String schemaName, String tableName, org.apache.arrow.vector.types.pojo.Schema tableSchema)
    {
        try {
            // Create schema-aware converter for table/column resolution
            CustomSubstraitToCalcite substraitToCalcite = new CustomSubstraitToCalcite(
                    SimpleExtension.load(loadDefaults()),
                    new SqlTypeFactoryImpl(sqlDialect.getTypeSystem()),
                    TypeConverter.DEFAULT,
                    tableName,
                    tableSchema
            );

            return convertSubstraitPlanToSql(planString, sqlDialect, substraitToCalcite);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to parse Substrait plan with schema: " + e.getMessage(), e);
        }
    }

    /**
     * Deserializes a Substrait plan with standard processing.
     * Uses standard SubstraitToCalcite converter.
     */
    public static SqlNode deserializeSubstraitPlan(String planString, SqlDialect sqlDialect)
    {
        try {
            // Create standard converter
            SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(
                    SimpleExtension.load(loadDefaults()),
                    new SqlTypeFactoryImpl(sqlDialect.getTypeSystem())
            );

            return convertSubstraitPlanToSql(planString, sqlDialect, substraitToCalcite);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to parse Substrait plan: " + e.getMessage(), e);
        }
    }

    private static List<String> loadDefaults() {
        return Stream.of("boolean", "aggregate_generic", "aggregate_approx", "arithmetic_decimal", "arithmetic", "comparison", "datetime", "logarithmic", "rounding", "rounding_decimal", "string")
                .map((func) -> String.format("/functions_%s.yaml", func)).collect(Collectors.toList());
    }

    /**
     * Common logic for converting Substrait plan to SQL using the provided converter.
     * Handles the core deserialization steps that are identical for both methods.
     */
    private static SqlNode convertSubstraitPlanToSql(String planString, SqlDialect sqlDialect, SubstraitToCalcite substraitToCalcite)
            throws Exception
    {
        // Step 1: Convert protobuf plan to Substrait plan object
        ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter();
        byte[] planBytes = Base64.getDecoder().decode(planString);
        Plan substraitPlan = Plan.parseFrom(planBytes);

        // Step 2: Convert Substrait plan to Calcite RelNode
        io.substrait.plan.Plan root = protoPlanConverter.from(substraitPlan);
        RelNode node = substraitToCalcite.convert(root.getRoots().get(0).getInput());

        // Step 3: Convert Calcite RelNode to SQL
        RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
        return converter.visitRoot(node).asStatement();
    }
}