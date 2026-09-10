/*-
 * #%L
 * athena-tpcds
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.tpcds;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import io.substrait.proto.Plan;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies the predicate carried by an Athena-generated Substrait query plan to TPC-DS rows as they are
 * generated.
 * <p>
 * On the managed (Athena federation) path the query's WHERE predicate arrives as a Substrait plan rather
 * than in the {@link Constraints} summary, and the engine does not re-apply it to the rows the connector
 * returns. This class parses that plan into a predicate tree and evaluates it per row so the connector
 * returns only matching rows. When the plan contains an operator this connector does not implement, the
 * SDK parser throws, surfacing a clear query error rather than silently returning unfiltered rows.
 */
public final class TPCDSSubstraitFilter
{
    private final LogicalExpression expression;
    private final Map<String, Column> columnsByName;

    private TPCDSSubstraitFilter(LogicalExpression expression, Map<String, Column> columnsByName)
    {
        this.expression = expression;
        this.columnsByName = columnsByName;
    }

    /**
     * Builds a filter from the request constraints, or returns null when there is no Substrait predicate
     * to apply (the legacy / Query Pass-Through path, or a plan with no filter).
     */
    public static TPCDSSubstraitFilter from(Constraints constraints, Table table)
    {
        if (constraints == null) {
            return null;
        }
        QueryPlan queryPlan = constraints.getQueryPlan();
        if (queryPlan == null || queryPlan.getSubstraitPlan() == null || queryPlan.getSubstraitPlan().isEmpty()) {
            return null;
        }

        Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(queryPlan.getSubstraitPlan());
        SubstraitRelModel relModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
        if (relModel.getFilterRel() == null) {
            return null;
        }

        List<String> columns = SubstraitMetadataParser.getTableColumns(relModel);
        LogicalExpression expression = SubstraitFunctionParser.parseLogicalExpression(
                plan.getExtensionsList(), relModel.getFilterRel().getCondition(), columns);
        if (expression == null) {
            return null;
        }

        Map<String, Column> columnsByName = new HashMap<>();
        for (Column column : table.getColumns()) {
            columnsByName.put(column.getName(), column);
        }
        return new TPCDSSubstraitFilter(expression, columnsByName);
    }

    /**
     * Package-private factory used by unit tests to build a filter from an already-parsed predicate tree.
     */
    static TPCDSSubstraitFilter forExpression(LogicalExpression expression, Table table)
    {
        Map<String, Column> columnsByName = new HashMap<>();
        for (Column column : table.getColumns()) {
            columnsByName.put(column.getName(), column);
        }
        return new TPCDSSubstraitFilter(expression, columnsByName);
    }

    /**
     * @param row A single TPC-DS row, indexed by column position.
     * @return True if the row satisfies the pushed-down predicate.
     */
    public boolean matches(List<String> row)
    {
        return evaluate(expression, row);
    }

    private boolean evaluate(LogicalExpression expr, List<String> row)
    {
        if (expr.isLeaf()) {
            return evaluateLeaf(expr.getLeafPredicate(), row);
        }
        switch (expr.getOperator()) {
            case AND:
                for (LogicalExpression child : expr.getChildren()) {
                    if (!evaluate(child, row)) {
                        return false;
                    }
                }
                return true;
            case OR:
                for (LogicalExpression child : expr.getChildren()) {
                    if (evaluate(child, row)) {
                        return true;
                    }
                }
                return false;
            default:
                throw new UnsupportedOperationException("Unsupported logical operator: " + expr.getOperator());
        }
    }

    private boolean evaluateLeaf(ColumnPredicate predicate, List<String> row)
    {
        SubstraitOperator operator = predicate.getOperator();
        if (operator == SubstraitOperator.NAND || operator == SubstraitOperator.NOR
                || operator == SubstraitOperator.NOT_IN) {
            return evaluateComposite(predicate, row);
        }

        Column column = resolveColumn(predicate.getColumn());
        String raw = row.get(column.getPosition());

        if (operator == SubstraitOperator.IS_NULL) {
            return raw == null;
        }
        if (operator == SubstraitOperator.IS_NOT_NULL) {
            return raw != null;
        }
        // A NULL value does not satisfy any comparison predicate (SQL three-valued logic).
        if (raw == null) {
            return false;
        }

        int cmp = compare(raw, predicate.getValue(), column.getType());
        switch (operator) {
            case EQUAL:
                return cmp == 0;
            case NOT_EQUAL:
                return cmp != 0;
            case GREATER_THAN:
                return cmp > 0;
            case GREATER_THAN_OR_EQUAL_TO:
                return cmp >= 0;
            case LESS_THAN:
                return cmp < 0;
            case LESS_THAN_OR_EQUAL_TO:
                return cmp <= 0;
            default:
                throw new UnsupportedOperationException("Unsupported comparison operator: " + operator);
        }
    }

    @SuppressWarnings("unchecked")
    private boolean evaluateComposite(ColumnPredicate predicate, List<String> row)
    {
        switch (predicate.getOperator()) {
            case NOT_IN: {
                Column column = resolveColumn(predicate.getColumn());
                String raw = row.get(column.getPosition());
                if (raw == null) {
                    return false;
                }
                for (Object value : (List<Object>) predicate.getValue()) {
                    if (compare(raw, value, column.getType()) == 0) {
                        return false;
                    }
                }
                return true;
            }
            case NAND: {
                boolean all = true;
                for (ColumnPredicate child : (List<ColumnPredicate>) predicate.getValue()) {
                    all &= evaluateLeaf(child, row);
                }
                return !all;
            }
            case NOR: {
                boolean any = false;
                for (ColumnPredicate child : (List<ColumnPredicate>) predicate.getValue()) {
                    any |= evaluateLeaf(child, row);
                }
                return !any;
            }
            default:
                throw new UnsupportedOperationException("Unsupported operator: " + predicate.getOperator());
        }
    }

    private Column resolveColumn(String columnName)
    {
        Column column = columnsByName.get(columnName);
        if (column == null) {
            throw new UnsupportedOperationException("Predicate references unknown column: " + columnName);
        }
        return column;
    }

    /**
     * Compares a TPC-DS raw string value against a Substrait literal using the column's type. Numeric
     * columns compare numerically, dates chronologically, and character columns lexicographically.
     */
    private int compare(String raw, Object literal, ColumnType type)
    {
        switch (type.getBase()) {
            case IDENTIFIER:
            case INTEGER:
            case DECIMAL:
                return new BigDecimal(raw).compareTo(new BigDecimal(String.valueOf(literal)));
            case DATE:
                return LocalDate.parse(raw).compareTo(toLocalDate(literal));
            case TIME:
            case CHAR:
            case VARCHAR:
            default:
                return raw.compareTo(String.valueOf(literal));
        }
    }

    private LocalDate toLocalDate(Object literal)
    {
        // Substrait date literals are represented as an epoch-day number; fall back to ISO text.
        if (literal instanceof Number) {
            return LocalDate.ofEpochDay(((Number) literal).longValue());
        }
        return LocalDate.parse(String.valueOf(literal));
    }
}
