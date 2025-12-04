/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.SubstraitRelUtils;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.LogicalExpression;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.google.cloud.bigquery.QueryParameterValue;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.google.bigquery.BigQuerySqlUtils.getValueForWhereClause;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.nCopies;

/**
 * Utilities for processing Substrait plans in BigQuery connector.
 */
public class BigQuerySubstraitPlanUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQuerySubstraitPlanUtils.class);
    private static final String BIGQUERY_QUOTE_CHAR = "`";

    private BigQuerySubstraitPlanUtils()
    {
    }

    /**
     * Builds filter predicates from Substrait plan.
     *
     * @param plan The Substrait plan
     * @return Map of column names to their predicates
     */
    public static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new HashMap<>();
        }

        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return new HashMap<>();
        }

        List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        return SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                tableColumns);
    }

    /**
     * Extracts limit from Substrait plan.
     *
     * @param plan The Substrait plan
     * @return The limit value, or 0 if no limit
     */
    public static int getLimit(Plan plan)
    {
        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
        FetchRel fetchRel = substraitRelModel.getFetchRel();
        return fetchRel != null ? (int) fetchRel.getCount() : 0;
    }

    /**
     * Extracts ORDER BY clause from Substrait plan.
     *
     * @param plan The Substrait plan
     * @return ORDER BY clause string, or empty string if no ordering
     */
    public static String extractOrderByClause(Plan plan)
    {
        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
        SortRel sortRel = substraitRelModel.getSortRel();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        if (sortRel == null || sortRel.getSortsCount() == 0) {
            return "";
        }
        return "ORDER BY " + sortRel.getSortsList().stream()
                .map(sortField -> {
                    String ordering = isAscending(sortField) ? "ASC" : "DESC";
                    String nullsHandling = sortField.getDirection().equals(SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST) ? "NULLS FIRST" : "NULLS LAST";
                    return quote(extractFieldIndexFromExpression(sortField.getExpr(), tableColumns)) + " " + ordering + " " + nullsHandling;
                })
                .collect(Collectors.joining(", "));
    }

    /**
     * Extracts the field index from a Substrait expression for column resolution.
     *
     * @param expression   The Substrait expression containing field reference
     * @param tableColumns List of table column names
     * @return The field name
     * @throws IllegalArgumentException if field index cannot be extracted from expression
     */
    public static String extractFieldIndexFromExpression(Expression expression, List<String> tableColumns)
    {
        if (expression == null) {
            throw new IllegalArgumentException("Expression cannot be null");
        }
        if (expression.hasSelection() && expression.getSelection().hasDirectReference()) {
            Expression.ReferenceSegment segment = expression.getSelection().getDirectReference();
            if (segment.hasStructField()) {
                int fieldIndex = segment.getStructField().getField();
                if (fieldIndex >= 0 && fieldIndex < tableColumns.size()) {
                    return tableColumns.get(fieldIndex).toLowerCase();
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract field from expression");
    }

    public static List<String> toConjuncts(List<Field> fields, Plan plan, Constraints constraints, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("toConjuncts called with {} fields", fields.size());
        List<String> conjuncts = new ArrayList<>();
        
        // Unified constraint processing - prioritize Substrait plan over summary
        if (constraints.getQueryPlan() != null) {
            LOGGER.info("Using Substrait plan for constraint processing");
            Plan substraitPlan = SubstraitRelUtils.deserializeSubstraitPlan(constraints.getQueryPlan().getSubstraitPlan());
            Map<String, List<ColumnPredicate>> columnPredicateMap = buildFilterPredicatesFromPlan(substraitPlan);
            
            if (!columnPredicateMap.isEmpty()) {
                try {
                    LOGGER.info("Attempting enhanced parameterized query generation from plan");
                    String query = makeEnhancedParameterizedQueryFromPlan(substraitPlan, fields, parameterValues);
                    if (query != null) {
                        conjuncts.add(query);
                        LOGGER.info("Enhanced query result: {}", query);
                    }
                }
                catch (Exception ex) {
                    LOGGER.warn("Enhanced query generation failed: {}", ex.getMessage());
                }
            }
        }
        
        LOGGER.info("toConjuncts returning {} conjuncts", conjuncts.size());
        return conjuncts;
    }

    /**
     * Enhanced parameterized query builder that tries tree-based approach first, then falls back to flattened approach
     */
    private static String makeEnhancedParameterizedQueryFromPlan(Plan plan, List<Field> columns, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("makeEnhancedParameterizedQueryFromPlan called with {} columns", columns.size());
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return null;
        }

        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            return null;
        }

        final List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        final List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);

        // Try tree-based approach first to preserve AND/OR logical structure
        try {
            final LogicalExpression logicalExpr = SubstraitFunctionParser.parseLogicalExpression(
                    extensionDeclarations,
                    substraitRelModel.getFilterRel().getCondition(),
                    columns.stream().map(Field::getName).collect(Collectors.toList()));

            if (logicalExpr != null) {
                String result = makeParameterizedQueryFromLogicalExpression(logicalExpr, parameterValues);
                LOGGER.info("Tree-based approach result: {}", result);
                return result;
            }
        }
        catch (Exception e) {
            LOGGER.warn("Tree-based parsing failed: {}", e.getMessage());
        }

        // Fall back to flattened approach
        final Map<String, List<ColumnPredicate>> predicates = SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                columns.stream().map(Field::getName).collect(Collectors.toList()));
        return makeParameterizedQueryFromPlan(predicates, columns, parameterValues);
    }

    /**
     * Converts a LogicalExpression tree to parameterized BigQuery SQL while preserving logical structure
     */
    private static String makeParameterizedQueryFromLogicalExpression(LogicalExpression logicalExpr, List<QueryParameterValue> parameterValues)
    {
        if (logicalExpr == null) {
            return null;
        }

        // Handle leaf nodes (individual predicates)
        if (logicalExpr.isLeaf()) {
            ColumnPredicate predicate = logicalExpr.getLeafPredicate();
            return convertColumnPredicateToParameterizedSql(predicate, parameterValues);
        }

        // Handle logical operators (AND/OR nodes with children)
        List<String> childClauses = new ArrayList<>();
        for (LogicalExpression child : logicalExpr.getChildren()) {
            String childClause = makeParameterizedQueryFromLogicalExpression(child, parameterValues);
            if (childClause != null && !childClause.trim().isEmpty()) {
                childClauses.add("(" + childClause + ")");
            }
        }

        if (childClauses.isEmpty()) {
            return null;
        }
        if (childClauses.size() == 1) {
            return childClauses.get(0);
        }

        // Apply the logical operator to combine child clauses
        if (logicalExpr.getOperator() == SubstraitOperator.AND) {
            return String.join(" AND ", childClauses);
        }
        return String.join(" OR ", childClauses);
    }

    /**
     * Processes column predicates from Substrait plan for parameterized queries
     */
    private static String makeParameterizedQueryFromPlan(Map<String, List<ColumnPredicate>> columnPredicates, List<Field> columns, List<QueryParameterValue> parameterValues)
    {
        if (columnPredicates == null || columnPredicates.isEmpty()) {
            return null;
        }
        
        List<String> predicates = new ArrayList<>();
        for (Field field : columns) {
            List<ColumnPredicate> fieldPredicates = columnPredicates.get(field.getName().toUpperCase());
            if (fieldPredicates != null) {
                for (ColumnPredicate predicate : fieldPredicates) {
                    String predicateClause = convertColumnPredicateToParameterizedSql(predicate, parameterValues);
                    if (predicateClause != null) {
                        predicates.add(predicateClause);
                    }
                }
            }
        }
        
        if (predicates.isEmpty()) {
            return null;
        }
        
        return predicates.size() == 1 ? predicates.get(0) : "(" + String.join(" AND ", predicates) + ")";
    }

    /**
     * Converts column predicates and ValueSet to parameterized SQL predicate
     */
    private static String toPredicate(String fieldName, Object fieldType, List<ColumnPredicate> predicates, Object summary, List<QueryParameterValue> parameterValues)
    {
        // If we have Substrait predicates, use them first
        if (predicates != null && !predicates.isEmpty()) {
            List<String> predicateStrings = new ArrayList<>();
            for (ColumnPredicate predicate : predicates) {
                String predicateStr = convertColumnPredicateToParameterizedSql(predicate, parameterValues);
                if (predicateStr != null) {
                    predicateStrings.add(predicateStr);
                }
            }
            if (!predicateStrings.isEmpty()) {
                return predicateStrings.size() == 1 ? predicateStrings.get(0) : "(" + String.join(" AND ", predicateStrings) + ")";
            }
        }
        
        // Fallback to ValueSet processing (similar to BigQuerySqlUtils)
        if (summary instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ValueSet> summaryMap = (Map<String, ValueSet>) summary;
            ValueSet valueSet = summaryMap.get(fieldName);
            if (valueSet != null && fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType) {
                return toPredicateFromValueSet(fieldName, valueSet, (org.apache.arrow.vector.types.pojo.ArrowType) fieldType, parameterValues);
            }
        }
        
        return null;
    }

    /**
     * Converts ValueSet to parameterized SQL predicate (adapted from BigQuerySqlUtils)
     */
    private static String toPredicateFromValueSet(String columnName, ValueSet valueSet, org.apache.arrow.vector.types.pojo.ArrowType type, List<QueryParameterValue> parameterValues)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", quote(columnName));
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", quote(columnName)));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", quote(columnName));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicateWithOperator(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicateWithOperator(columnName, ">=", range.getLow().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                rangeConjuncts.add(toPredicateWithOperator(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicateWithOperator(columnName, "<", range.getHigh().getValue(), type, parameterValues));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + String.join(" AND ", rangeConjuncts) + ")");
                }
            }

            // Handle single values
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicateWithOperator(columnName, "=", getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(getValueForWhereClause(columnName, value, type));
                }
                String values = String.join(",", nCopies(singleValues.size(), "?"));
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + String.join(" OR ", disjuncts) + ")";
    }

    /**
     * Creates parameterized predicate with operator
     */
    private static String toPredicateWithOperator(String columnName, String operator, Object value, org.apache.arrow.vector.types.pojo.ArrowType type, List<QueryParameterValue> parameterValues)
    {
        parameterValues.add(getValueForWhereClause(columnName, value, type));
        return quote(columnName) + " " + operator + " ?";
    }

    /**
     * Converts ColumnPredicate to parameterized SQL using List instead of Map
     */
    private static String convertColumnPredicateToParameterizedSql(ColumnPredicate predicate, List<QueryParameterValue> parameterValues)
    {
        String columnName = quote(predicate.getColumn());
        Object value = predicate.getValue();
        
        switch (predicate.getOperator()) {
            case EQUAL:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " = ?";
            case NOT_EQUAL:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " != ?";
            case GREATER_THAN:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " > ?";
            case GREATER_THAN_OR_EQUAL_TO:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " >= ?";
            case LESS_THAN:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " < ?";
            case LESS_THAN_OR_EQUAL_TO:
                parameterValues.add(getValueForWhereClause(predicate.getColumn(), value, predicate.getArrowType()));
                return columnName + " <= ?";
            case IS_NULL:
                return columnName + " IS NULL";
            case IS_NOT_NULL:
                return columnName + " IS NOT NULL";
            default:
                return null;
        }
    }

    /**
     * Determines if a sort field is in ascending order based on Substrait sort direction.
     *
     * @param sortField The Substrait sort field to check
     * @return true if sort direction is ascending, false if descending
     */
    public static boolean isAscending(SortField sortField)
    {
        return sortField.getDirection() == SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST ||
                sortField.getDirection() == SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST;
    }

    private static String quote(final String identifier)
    {
        return BIGQUERY_QUOTE_CHAR + identifier + BIGQUERY_QUOTE_CHAR;
    }
}
