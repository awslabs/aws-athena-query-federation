/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.substrait;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Evaluates Substrait FilterRel expressions against rows in an Arrow VectorSchemaRoot.
 * <p>
 * Uses {@code parseLogicalExpression} to build a full logical expression tree (AND/OR/NOT)
 * and evaluates it recursively for each row.
 * <p>
 * If the Substrait plan is not available or parsing fails, the filter passes all rows (no filtering).
 */
public class SubstraitRowFilter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SubstraitRowFilter.class);

    private final LogicalExpression logicalExpression;
    private final boolean hasFilter;

    private SubstraitRowFilter(LogicalExpression logicalExpression, boolean hasFilter)
    {
        this.logicalExpression = logicalExpression;
        this.hasFilter = hasFilter;
    }

    /**
     * Creates a SubstraitRowFilter from a QueryPlan object.
     * Returns a no-op filter if the plan is null or has no FilterRel.
     */
    public static SubstraitRowFilter fromQueryPlan(Constraints constraints)
    {
        if (constraints.getQueryPlan() == null || constraints.getQueryPlan().getSubstraitPlan() == null) {
            LOGGER.info("In SubstraitRowFilter, QueryPlan/SubstraitPlan is null");
            return noOpFilter();
        }

        try {
            Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(
                constraints.getQueryPlan().getSubstraitPlan());
                
            SubstraitRelModel model = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());

            if (model.getFilterRel() == null || model.getFilterRel().getCondition() == null) {
                LOGGER.info("In SubstraitRowFilter, no FilterRel/Condition found, returning no-op filter");
                return noOpFilter();
            }

            Expression filterExpression = model.getFilterRel().getCondition();
            List<SimpleExtensionDeclaration> extensions = plan.getExtensionsList();
            List<String> tableColumns = SubstraitMetadataParser.getTableColumns(model);

            // Parse the filter into a LogicalExpression tree
            LogicalExpression logicalExpr = SubstraitFunctionParser.parseLogicalExpression(
                extensions, filterExpression, tableColumns);

            if (logicalExpr == null) {
                LOGGER.info("LogicalExpression is null, returning no-op filter");
                return noOpFilter();
            }

            LOGGER.info("--- Logical Expression Tree ---");
            logExpressionTree(logicalExpr, 0);

            return new SubstraitRowFilter(logicalExpr, true);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to build SubstraitRowFilter, returning no-op filter", e);
            return noOpFilter();
        }
    }

    /**
     * Recursively logs the LogicalExpression tree with indentation to visualize the structure.
     */
    private static void logExpressionTree(LogicalExpression expr, int depth)
    {
        String indent = "  " + "  ".repeat(depth);
        if (expr == null) {
            LOGGER.info("{}(null)", indent);
            return;
        }
        if (expr.isLeaf()) {
            ColumnPredicate pred = expr.getLeafPredicate();
            LOGGER.info("{}LEAF: {} {} '{}' [type={}]",
                indent, pred.getColumn(), pred.getOperator(), pred.getValue(),
                pred.getArrowType() != null ? pred.getArrowType() : "unknown");
        }
        else {
            LOGGER.info("{}{} (children={})", indent, expr.getOperator(),
                expr.getChildren() != null ? expr.getChildren().size() : 0);
            if (expr.getChildren() != null) {
                for (LogicalExpression child : expr.getChildren()) {
                    logExpressionTree(child, depth + 5);
                }
            }
        }
    }

    /**
     * Returns a filter that passes all rows.
     */
    public static SubstraitRowFilter noOpFilter()
    {
        return new SubstraitRowFilter(null, false);
    }

    /**
     * Returns true if this filter has predicates to evaluate.
     */
    public boolean hasFilter()
    {
        return hasFilter;
    }

    /**
     * Evaluates whether the row at the given index passes the filter.
     *
     * @param root     The VectorSchemaRoot containing the batch data
     * @param rowIndex The row index to evaluate
     * @return true if the row passes the filter (should be included), false if it should be skipped
     */
    public boolean evaluate(VectorSchemaRoot root, int rowIndex)
    {
        if (!hasFilter) {
            return true;
        }
        return evaluateLogicalExpression(logicalExpression, root, rowIndex);
    }

    /**
     * Evaluates a LogicalExpression tree recursively.
     */
    private boolean evaluateLogicalExpression(LogicalExpression expr, VectorSchemaRoot root, int rowIndex)
    {
        if (expr == null) {
            return true;
        }

        if (expr.isLeaf()) {
            ColumnPredicate predicate = expr.getLeafPredicate();
            SubstraitOperator op = predicate.getOperator();

            // Handle NOR/NAND/NOT_IN compound predicates which have null column
            // and store child predicates in their value field
            if (op == SubstraitOperator.NOR) {
                return evaluateNorPredicate(predicate, root, rowIndex);
            }
            if (op == SubstraitOperator.NAND) {
                return evaluateNandPredicate(predicate, root, rowIndex);
            }
            if (op == SubstraitOperator.NOT_IN) {
                return evaluateNotInPredicate(predicate, root, rowIndex);
            }

            FieldVector vector = root.getVector(predicate.getColumn());
            if (vector == null) {
                return true; // Column not present, can't filter
            }
            Object value = vector.getObject(rowIndex);
            return evaluateSinglePredicate(value, predicate);
        }

        SubstraitOperator operator = expr.getOperator();
        List<LogicalExpression> children = expr.getChildren();

        if (operator == SubstraitOperator.AND) {
            for (LogicalExpression child : children) {
                if (!evaluateLogicalExpression(child, root, rowIndex)) {
                    return false; // short-circuit AND
                }
            }
            return true;
        }
        else if (operator == SubstraitOperator.OR) {
            for (LogicalExpression child : children) {
                if (evaluateLogicalExpression(child, root, rowIndex)) {
                    return true; // short-circuit OR
                }
            }
            return false;
        }
        else if (operator == SubstraitOperator.NOT) {
            if (children != null && !children.isEmpty()) {
                return !evaluateLogicalExpression(children.get(0), root, rowIndex);
            }
            return true;
        }

        // Unknown operator at tree level — pass the row
        return true;
    }

    /**
     * Evaluates a single predicate against a value.
     */
    private boolean evaluateSinglePredicate(Object value, ColumnPredicate predicate)
    {
        SubstraitOperator operator = predicate.getOperator();

        switch (operator) {
            case IS_NULL:
                return value == null;
            case IS_NOT_NULL:
                return value != null;
            case EQUAL:
                return value != null && compareValues(value, predicate.getValue()) == 0;
            case NOT_EQUAL:
                return value == null || compareValues(value, predicate.getValue()) != 0;
            case GREATER_THAN:
                return value != null && compareValues(value, predicate.getValue()) > 0;
            case GREATER_THAN_OR_EQUAL_TO:
                return value != null && compareValues(value, predicate.getValue()) >= 0;
            case LESS_THAN:
                return value != null && compareValues(value, predicate.getValue()) < 0;
            case LESS_THAN_OR_EQUAL_TO:
                return value != null && compareValues(value, predicate.getValue()) <= 0;
            case NOT_IN:
                return evaluateNotIn(value, predicate.getValue());
            default:
                // Unsupported operator — don't filter
                LOGGER.debug("Unsupported Substrait operator {}, allowing row to pass", operator);
                return true;
        }
    }

    /**
     * Evaluates a NOT IN predicate.
     * Returns true if the value is NOT contained in the exclusion list.
     */
    private boolean evaluateNotIn(Object value, Object excludedValues)
    {
        if (value == null) {
            return true; // NULL is not considered to be IN any list
        }
        if (!(excludedValues instanceof java.util.List)) {
            LOGGER.debug("NOT_IN predicate value is not a List, allowing row to pass");
            return true;
        }
        java.util.List<?> exclusionList = (java.util.List<?>) excludedValues;
        for (Object excluded : exclusionList) {
            if (compareValues(value, excluded) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Evaluates a NOR predicate against the vector schema root.
     * NOR means NOT(p1 OR p2 OR p3) — the row passes only if NONE of the children match.
     * Each child predicate may reference its own column.
     */
    @SuppressWarnings("unchecked")
    private boolean evaluateNorPredicate(ColumnPredicate predicate, VectorSchemaRoot root, int rowIndex)
    {
        Object predicateValue = predicate.getValue();
        if (!(predicateValue instanceof java.util.List)) {
            LOGGER.debug("NOR predicate value is not a List, allowing row to pass");
            return true;
        }
        java.util.List<ColumnPredicate> childPredicates = (java.util.List<ColumnPredicate>) predicateValue;
        for (ColumnPredicate child : childPredicates) {
            FieldVector vector = root.getVector(child.getColumn());
            if (vector == null) {
                continue; // Column not present, skip this child
            }
            Object value = vector.getObject(rowIndex);
            if (evaluateSinglePredicate(value, child)) {
                return false; // One child matched, NOR fails (row excluded)
            }
        }
        return true;
    }

    /**
     * Evaluates a NAND predicate against the vector schema root.
     * NAND means NOT(p1 AND p2 AND p3) — the row passes if NOT ALL children match.
     */
    @SuppressWarnings("unchecked")
    private boolean evaluateNandPredicate(ColumnPredicate predicate, VectorSchemaRoot root, int rowIndex)
    {
        Object predicateValue = predicate.getValue();
        if (!(predicateValue instanceof java.util.List)) {
            LOGGER.debug("NAND predicate value is not a List, allowing row to pass");
            return true;
        }
        java.util.List<ColumnPredicate> childPredicates = (java.util.List<ColumnPredicate>) predicateValue;
        for (ColumnPredicate child : childPredicates) {
            FieldVector vector = root.getVector(child.getColumn());
            if (vector == null) {
                return true; // Column not present, can't satisfy all children
            }
            Object value = vector.getObject(rowIndex);
            if (!evaluateSinglePredicate(value, child)) {
                return true; // One child doesn't match, NAND passes
            }
        }
        return false; // All children matched, NAND fails
    }

    /**
     * Evaluates a NOT_IN predicate against the vector schema root.
     * The predicate value is a list of excluded values for the target column.
     */
    private boolean evaluateNotInPredicate(ColumnPredicate predicate, VectorSchemaRoot root, int rowIndex)
    {
        String column = predicate.getColumn();
        if (column == null) {
            return true;
        }
        FieldVector vector = root.getVector(column);
        if (vector == null) {
            return true;
        }
        Object value = vector.getObject(rowIndex);
        return evaluateNotIn(value, predicate.getValue());
    }

    /**
     * Compares two values using their natural ordering.
     * Handles type coercion between the row value and the predicate value.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValues(Object rowValue, Object predicateValue)
    {
        if (rowValue == null && predicateValue == null) {
            return 0;
        }
        if (rowValue == null) {
            return -1;
        }
        if (predicateValue == null) {
            return 1;
        }

        // Try direct Comparable comparison
        if (rowValue instanceof Comparable && predicateValue instanceof Comparable) {
            try {
                return ((Comparable) rowValue).compareTo(predicateValue);
            }
            catch (ClassCastException e) {
                // Types don't match directly — try numeric coercion or fall back to string
            }
        }

        // Numeric coercion: if both are numbers, compare as doubles
        if (rowValue instanceof Number && predicateValue instanceof Number) {
            return Double.compare(((Number) rowValue).doubleValue(), ((Number) predicateValue).doubleValue());
        }

        // Last resort: compare as strings
        return rowValue.toString().compareTo(predicateValue.toString());
    }
}
