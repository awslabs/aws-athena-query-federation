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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    
    public static Map<String, String> getColumnRemapping(final String planString, final SqlDialect sqlDialect)
    {
        final Plan protoPlan = SubstraitRelUtils.deserializeSubstraitPlan(planString);
        final Map<String, String> columnRemapping = new LinkedHashMap<>();
        final RelNode relNode = getRelNodeFromSubstraitPlan(protoPlan, sqlDialect);
        RelDataTypeFactory.Builder builder = relNode.getCluster().getTypeFactory().builder();
        traverse(relNode, builder, columnRemapping);
        LOGGER.debug("Column rename mapping (original → renamed): {}", columnRemapping);
        return columnRemapping;
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
        traverse(relNode, builder, new LinkedHashMap<>());
        
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
    
    /**
     * Traverses the RelNode tree, adding fields to the builder only for TableScan (leaf)
     * or Project/Aggregate (select with renaming) nodes, and builds a column rename mapping.
     * <p>
     * The rename mapping tracks how output column names map back to the original
     * base table column names. For example:
     * <ul>
     *   <li>{@code varchar_col0 → varchar_col} (direct column reference renamed by Calcite)</li>
     *   <li>{@code $f24 → null} (computed expression, no direct column mapping)</li>
     * </ul>
     *
     * @param node The current RelNode being traversed
     * @param builder The builder collecting the schema fields
     * @param renameMapping Output map: renamed column name → original base table column name (null for computed expressions)
     */
    private static void traverse(RelNode node, RelDataTypeFactory.Builder builder, Map<String, String> renameMapping)
    {
        if (node.getInputs().isEmpty()) {
            // Case 1: TableScan (leaf node) - add base table fields
            builder.addAll(node.getRowType().getFieldList());
        }
        else if (node instanceof Project) {
            // Case 2: Project (SELECT with renaming) - add renamed fields and build rename mapping
            Project project = (Project) node;
            List<RelDataTypeField> inputFields = project.getInput().getRowType().getFieldList();
            List<RelDataTypeField> outputFields = project.getRowType().getFieldList();

            // Add the renamed column names and their types to the builder
            builder.addAll(outputFields);

            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode rex = project.getProjects().get(i);
                String outputName = outputFields.get(i).getName();
                if (rex instanceof RexInputRef) {
                    int inputIndex = ((RexInputRef) rex).getIndex();
                    String originalName = inputFields.get(inputIndex).getName();
                    if (!outputName.equals(originalName)) {
                        renameMapping.put(outputName, originalName);
                    }
                }
                else {
                    renameMapping.put(outputName, null);
                }
            }
        }
        else if (node instanceof Aggregate) {
            // Case 2b: Aggregate (GROUP BY with renaming) - add renamed fields and build rename mapping
            Aggregate aggregate = (Aggregate) node;
            List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();
            List<RelDataTypeField> outputFields = aggregate.getRowType().getFieldList();

            // Add the renamed column names and their types to the builder
            builder.addAll(outputFields);

            int outputIdx = 0;
            // Group key columns map back to input columns via groupSet indices
            for (int groupIndex : aggregate.getGroupSet()) {
                String outputName = outputFields.get(outputIdx).getName();
                String originalName = inputFields.get(groupIndex).getName();
                if (!outputName.equals(originalName)) {
                    renameMapping.put(outputName, originalName);
                }
                outputIdx++;
            }
            
            // Aggregate function columns (SUM, COUNT, AVG, etc.) are computed expressions with no base column mapping
            while (outputIdx < outputFields.size()) {
                String outputName = outputFields.get(outputIdx).getName();
                renameMapping.put(outputName, null);
                outputIdx++;
            }
        }

        // Recurse into child nodes
        for (RelNode input : node.getInputs()) {
            traverse(input, builder, renameMapping);
        }

        // Resolve transitive renames: if output maps to intermediate, and intermediate maps further, follow the chain
        for (Map.Entry<String, String> entry : renameMapping.entrySet()) {
            String intermediate = entry.getValue();
            if (intermediate != null && renameMapping.containsKey(intermediate)) {
                entry.setValue(renameMapping.get(intermediate));
            }
        }
    }
}
