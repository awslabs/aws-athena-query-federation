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
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for converting Substrait plans to SQL and extracting schema information.
 * <p>
 * Provides methods to:
 * - Convert Substrait plans (serialized as base64 strings) to Calcite SqlNode representations
 * - Extract Arrow Schema from Substrait plans
 * - Convert between Calcite RelDataType and Apache Arrow types
 * <p>
 * This class uses Apache Calcite as an intermediate representation to bridge Substrait plans
 * and SQL dialects, enabling connectors to work with query pushdown capabilities.
 */

public final class SubstraitSqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitSqlUtils.class);

    private SubstraitSqlUtils()
    {
    }

    public static SqlNode getSqlNodeFromSubstraitPlan(final String planString, final SqlDialect sqlDialect)
    {
        try {
            final Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
            return getSqlNodeFromSubstraitPlan(protoPlan, sqlDialect);
        }
        catch (final Exception e) {
            LOGGER.error("Failed to parse Substrait plan", e);
            throw new RuntimeException("Failed to parse Substrait plan", e);
        }
    }

    private static SqlNode getSqlNodeFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        final RelNode node = getRelNodeFromSubstraitPlan(protoPlan, sqlDialect);
        final RelToSqlConverter converter = new RelToSqlConverter(sqlDialect);
        return converter.visitRoot(node).asStatement();
    }

    public static RelDataType getTableSchemaFromSubstraitPlan(final String planString, final SqlDialect sqlDialect)
    {
        try {
            final Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
            return getTableSchemaFromSubstraitPlan(protoPlan, sqlDialect);
        }
        catch (final Exception e) {
            LOGGER.error("Failed to extract table schema from Substrait plan", e);
            throw new RuntimeException("Failed to extract table schema from Substrait plan", e);
        }
    }

    private static RelDataType getTableSchemaFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        final Rel rel = protoPlan.getRelations(0).getRoot().getInput();
        final ReadRel readRel = SubstraitRelUtils.getReadRel(rel);

        if (readRel == null || !readRel.hasBaseSchema()) {
            throw new RuntimeException("Unable to extract base table schema from Substrait plan");
        }
        
        final RelNode relNode = getRelNodeFromSubstraitPlan(protoPlan, sqlDialect);
        RelDataTypeFactory.Builder builder = relNode.getCluster().getTypeFactory().builder();
        traverse(relNode, builder);
        
        return builder.build();
    }

    private static RelNode getRelNodeFromSubstraitPlan(final Plan protoPlan, final SqlDialect sqlDialect)
    {
        try {
            final ProtoPlanConverter protoPlanConverter = new ProtoPlanConverter();
            final io.substrait.plan.Plan substraitPlan = protoPlanConverter.from(protoPlan);

            final SubstraitToCalcite substraitToCalcite = new SubstraitToCalcite(
                    SimpleExtension.loadDefaults(),
                    new SqlTypeFactoryImpl(sqlDialect.getTypeSystem())
            );

            return substraitToCalcite.convert(substraitPlan.getRoots().get(0).getInput());
        }
        catch (final Exception e) {
            LOGGER.error("Failed to convert from Substrait plan to RelNode", e);
            throw new RuntimeException("Failed to convert from Substrait plan to RelNode", e);
        }
    }
    
    private static void traverse(RelNode node, RelDataTypeFactory.Builder builder)
    {
        RelDataType schema = node.getRowType();
        builder.addAll(schema.getFieldList());
        for (RelNode input : node.getInputs()) {
            traverse(input, builder);
        }
    }
}
