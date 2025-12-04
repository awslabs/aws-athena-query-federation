
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
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Utf8;

/**
 * Utilities that help with Sql operations.
 */
public class BigQueryStorageApiUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryStorageApiUtils.class);

    private static final String BIGQUERY_QUOTE_CHAR = "\"";

    private BigQueryStorageApiUtils()
    {
    }

    private static String quote(final String identifier)
    {
        return BIGQUERY_QUOTE_CHAR + identifier + BIGQUERY_QUOTE_CHAR;
    }

    private static List<String> toConjuncts(List<Field> columns, Constraints constraints)
    {
        LOGGER.info("toConjuncts called with {} columns and constraints: {}", columns.size(), constraints);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        String query = null;
        // Unified constraint processing - prioritize Substrait plan over summary
        if (constraints.getQueryPlan() != null) {
            LOGGER.info("Using Substrait plan for constraint processing");
            Plan plan = SubstraitRelUtils.deserializeSubstraitPlan(constraints.getQueryPlan().getSubstraitPlan());
            Map<String, List<ColumnPredicate>> columnPredicateMap = BigQuerySubstraitPlanUtils.buildFilterPredicatesFromPlan(plan);
            LOGGER.info("Column predicate map: {}", columnPredicateMap);
            if (!columnPredicateMap.isEmpty()) {
                // Use enhanced query generation that preserves AND/OR logical structure from SQL
                // This handles cases like "job_title IN ('A', 'B') OR job_title < 'C'" correctly as OR operations
                // instead of flattening them into AND operations like the legacy approach
                try {
                    LOGGER.info("Attempting enhanced query generation from plan");
                    query = makeEnhancedQueryFromPlan(plan, columns);
                    LOGGER.info("Enhanced query result: {}", query);
                }
                catch (Exception ex) {
                    LOGGER.warn("Enhanced query generation failed, falling back to basic plan query: {}", ex.getMessage());
                    query = makeQueryFromPlan(columnPredicateMap, columns);
                    LOGGER.info("Basic plan query result: {}", query);
                }
            }
            else {
                LOGGER.info("Column predicate map is empty, no query generated from plan");
            }
        }
        else if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
            LOGGER.info("Using constraint summary for processing with {} entries", constraints.getSummary().size());
            // Fallback to summary-based processing
            for (Field column : columns) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.info("Processing valueSet for column {}: {}", column.getName(), valueSet);
                    query = toPredicate(column.getName(), valueSet, column.getType());
                    LOGGER.info("Generated predicate for column {}: {}", column.getName(), query);
                }
            }
        }
        else {
            LOGGER.info("No query plan or constraint summary available");
        }
        
        if (query != null) {
            LOGGER.info("Adding query to builder: {}", query);
            builder.add(query);
        }
        else {
            LOGGER.info("No query generated from constraints");
        }
        
        List<String> complexExpressions = new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints);
        LOGGER.info("Complex expressions parsed: {}", complexExpressions);
        builder.addAll(complexExpressions);
        
        List<String> result = builder.build();
        LOGGER.info("toConjuncts returning {} clauses: {}", result.size(), result);
        return result;
    }

    private static String makeEnhancedQueryFromPlan(Plan plan, List<Field> columns)
    {
        LOGGER.info("makeEnhancedQueryFromPlan called columns count: {}", columns.size());
        if (plan == null || plan.getRelationsList().isEmpty()) {
            LOGGER.info("Plan is null or has no relations, returning null");
            return null;
        }

        SubstraitRelModel substraitRelModel = SubstraitRelModel.buildSubstraitRelModel(
                plan.getRelations(0).getRoot().getInput());
        if (substraitRelModel.getFilterRel() == null) {
            LOGGER.info("No filter relation found in substrait model, returning null");
            return null;
        }

        final List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        final List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
        LOGGER.info("Extension declarations count: {}, table columns: {}", extensionDeclarations.size(), tableColumns);

        // Try tree-based approach first to preserve AND/OR logical structure
        // This handles cases like "A OR B OR C" correctly as OR operations
        try {
            LOGGER.info("Attempting tree-based parsing approach");
            final LogicalExpression logicalExpr = SubstraitFunctionParser.parseLogicalExpression(
                    extensionDeclarations,
                    substraitRelModel.getFilterRel().getCondition(),
                    columns.stream().map(Field::getName).collect(Collectors.toList()));

            if (logicalExpr != null) {
                LOGGER.info("Successfully parsed logical expression, converting to BigQuery query");
                // Successfully parsed expression tree - convert to GoogleBigQuery query
                String result = makeQueryFromLogicalExpression(logicalExpr);
                LOGGER.info("Tree-based approach result: {}", result);
                return result;
            }
            else {
                LOGGER.info("Logical expression is null, falling back to flattened approach");
            }
        }
        catch (Exception e) {
            LOGGER.warn("Tree-based parsing failed - fall back to flattened approach  {}", e.getMessage());
        }

        // Fall back to existing flattened approach for backward compatibility
        // This maintains support for edge cases where tree-based parsing might fail
        LOGGER.info("Using flattened approach for backward compatibility");
        final Map<String, List<ColumnPredicate>> predicates = SubstraitFunctionParser.getColumnPredicatesMap(
                extensionDeclarations,
                substraitRelModel.getFilterRel().getCondition(),
                tableColumns);
        LOGGER.info("Extracted predicates map: {}", predicates);
        String result = makeQueryFromPlan(predicates, columns);
        LOGGER.info("Flattened approach result: {}", result);
        return result;
    }

    private static String makeQueryFromLogicalExpression(LogicalExpression logicalExpr)
    {
        LOGGER.info("makeQueryFromLogicalExpression called with expression: {}", logicalExpr);
        if (logicalExpr == null) {
            LOGGER.info("LogicalExpression is null, returning null");
            return null;
        }

        // Handle leaf nodes (individual predicates like job_title = 'Engineer')
        if (logicalExpr.isLeaf()) {
            LOGGER.info("Processing leaf node with predicate: {}", logicalExpr.getLeafPredicate());
            ColumnPredicate predicate = logicalExpr.getLeafPredicate();
            String result = convertColumnPredicateToSql(predicate);
            LOGGER.info("Leaf node converted to SQL: {}", result);
            return result;
        }

        // Handle logical operators (AND/OR nodes with children)
        LOGGER.info("Processing logical operator: {} with {} children", logicalExpr.getOperator(), logicalExpr.getChildren().size());
        List<String> childClauses = new ArrayList<>();
        for (LogicalExpression child : logicalExpr.getChildren()) {
            String childClause = makeQueryFromLogicalExpression(child);
            if (childClause != null && !childClause.trim().isEmpty()) {
                childClauses.add("(" + childClause + ")");
                LOGGER.info("Added child clause: {}", childClause);
            }
        }

        if (childClauses.isEmpty()) {
            LOGGER.info("No valid child clauses found, returning null");
            return null;
        }
        if (childClauses.size() == 1) {
            LOGGER.info("Single child clause, returning: {}", childClauses.get(0));
            return childClauses.get(0);
        }

        // Apply the logical operator to combine child clauses
        if (requireNonNull(logicalExpr.getOperator()) == SubstraitOperator.AND) {
            String result = String.join(" AND ", childClauses);
            LOGGER.info("Combined with AND operator: {}", result);
            return result;
        }
        String result = String.join(" OR ", childClauses);
        LOGGER.info("Combined with OR operator: {}", result);
        return result;
    }

    private static String convertColumnPredicateToSql(ColumnPredicate predicate)
    {
        LOGGER.info("convertColumnPredicateToSql called with predicate: column={}, operator={}, value={}", 
                    predicate.getColumn(), predicate.getOperator(), predicate.getValue());
        String columnName = predicate.getColumn();
        Object value = predicate.getValue();
        
        switch (predicate.getOperator()) {
            case EQUAL:
                String equalResult = columnName + " = " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("EQUAL operator result: {}", equalResult);
                return equalResult;
            case NOT_EQUAL:
                String notEqualResult = columnName + " != " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("NOT_EQUAL operator result: {}", notEqualResult);
                return notEqualResult;
            case GREATER_THAN:
                String gtResult = columnName + " > " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("GREATER_THAN operator result: {}", gtResult);
                return gtResult;
            case GREATER_THAN_OR_EQUAL_TO:
                String gteResult = columnName + " >= " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("GREATER_THAN_OR_EQUAL_TO operator result: {}", gteResult);
                return gteResult;
            case LESS_THAN:
                String ltResult = columnName + " < " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("LESS_THAN operator result: {}", ltResult);
                return ltResult;
            case LESS_THAN_OR_EQUAL_TO:
                String lteResult = columnName + " <= " + formatValueForStorageApi(value, predicate.getArrowType());
                LOGGER.info("LESS_THAN_OR_EQUAL_TO operator result: {}", lteResult);
                return lteResult;
            case IS_NULL:
                String nullResult = columnName + " IS NULL";
                LOGGER.info("IS_NULL operator result: {}", nullResult);
                return nullResult;
            case IS_NOT_NULL:
                String notNullResult = columnName + " IS NOT NULL";
                LOGGER.info("IS_NOT_NULL operator result: {}", notNullResult);
                return notNullResult;
            default:
                LOGGER.info("Unsupported operator: {}, returning null", predicate.getOperator());
                return null;
        }
    }

    private static String parseOrExpression(Expression expr, List<String> tableColumns)
    {
        if (!expr.hasScalarFunction()) {
            return null;
        }

        Expression.ScalarFunction scalarFunc = expr.getScalarFunction();
        
        // Check if this is an OR function (function reference 0)
        if (scalarFunc.getFunctionReference() == 0) {
            List<String> orTerms = new ArrayList<>();
            
            for (io.substrait.proto.FunctionArgument arg : scalarFunc.getArgumentsList()) {
                if (arg.hasValue()) {
                    String term = parseComparisonExpression(arg.getValue(), tableColumns);
                    if (term != null) {
                        orTerms.add("(" + term + ")");
                    }
                }
            }
            
            if (orTerms.size() > 1) {
                return "(" + String.join(" OR ", orTerms) + ")";
            }
        }
        
        return null;
    }

    private static String parseComparisonExpression(Expression expr, List<String> tableColumns)
    {
        if (!expr.hasScalarFunction()) {
            return null;
        }

        Expression.ScalarFunction scalarFunc = expr.getScalarFunction();
        
        // Check if this is an equality function (function reference 1)
        if (scalarFunc.getFunctionReference() == 1 && scalarFunc.getArgumentsCount() == 2) {
            io.substrait.proto.FunctionArgument leftArg = scalarFunc.getArguments(0);
            io.substrait.proto.FunctionArgument rightArg = scalarFunc.getArguments(1);
            
            if (leftArg.hasValue() && rightArg.hasValue()) {
                String columnName = BigQuerySubstraitPlanUtils.extractFieldIndexFromExpression(leftArg.getValue(), tableColumns);
                String value = extractLiteralValue(rightArg.getValue());
            
                if (columnName != null && value != null) {
                    return columnName + " = '" + value + "'";
                }
            }
        }
        
        return null;
    }

    private static String extractLiteralValue(Expression expr)
    {
        if (expr.hasLiteral()) {
            Expression.Literal literal = expr.getLiteral();
            if (literal.hasString()) {
                return literal.getString();
            }
        }
        return null;
    }

    /**
     * Processes column predicates from Substrait plan for Storage API row restrictions.
     */
    private static String makeQueryFromPlan(Map<String, List<ColumnPredicate>> columnPredicates, List<Field> columns)
    {
        LOGGER.info("makeQueryFromPlan called with {} column predicates and {} columns", 
                    columnPredicates != null ? columnPredicates.size() : 0, columns.size());
        if (columnPredicates == null || columnPredicates.isEmpty()) {
            LOGGER.info("Column predicates is null or empty, returning null");
            return null;
        }
        
        List<String> predicates = new ArrayList<>();
        for (Field field : columns) {
            List<ColumnPredicate> fieldPredicates = columnPredicates.get(field.getName().toUpperCase());
            if (fieldPredicates != null) {
                LOGGER.info("Processing {} predicates for field: {}", fieldPredicates.size(), field.getName());
                for (ColumnPredicate predicate : fieldPredicates) {
                    String operator = predicate.getOperator().getSymbol();
                    String predicateClause;
                    if (predicate.getOperator() == SubstraitOperator.IS_NULL ||
                            predicate.getOperator() == SubstraitOperator.IS_NOT_NULL) {
                        predicateClause = quote(predicate.getColumn()) + " " + operator;
                        LOGGER.info("Created null check predicate: {}", predicateClause);
                    }
                    else {
                        predicateClause = createPredicateClause(predicate.getColumn(), operator, predicate.getValue(), predicate.getArrowType());
                        LOGGER.info("Created value predicate: {}", predicateClause);
                    }
                    predicates.add(predicateClause);
                }
            }
            else {
                LOGGER.info("No predicates found for field: {}", field.getName());
            }
        }
        
        if (predicates.isEmpty()) {
            LOGGER.info("No predicates generated, returning null");
            return null;
        }
        
        String result = predicates.size() == 1 ? predicates.get(0) : "(" + String.join(" AND ", predicates) + ")";
        LOGGER.info("makeQueryFromPlan result: {}", result);
        return result;
    }

    /**
     * Creates predicate clause for Storage API row restrictions.
     */
    private static String createPredicateClause(String columnName, String operator, Object value, ArrowType type)
    {
        String formattedValue = formatValueForStorageApi(value, type);
        return quote(columnName) + " " + operator + " " + formattedValue;
    }

    /**
     * Formats values for Storage API row restrictions.
     */
    private static String formatValueForStorageApi(Object value, ArrowType type)
    {
        if (value == null) {
            return "NULL";
        }
        
        switch (type.getTypeID()) {
            case Utf8:
                return "'" + value.toString().replace("\"", "\\\"") + "'";
            case Int:
            case FloatingPoint:
            case Decimal:
                return value.toString();
            case Bool:
                return value.toString().toUpperCase();
            default:
                return "'" + value + "'";
        }
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("%s IS NULL", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("%s IS NULL", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("%s IS NOT NULL", columnName);
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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add(Joiner.on(" AND ").join(rangeConjuncts));
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type));
            }
            else if (singleValues.size() > 1) {
                List<String> val = new ArrayList<>();
                for (Object value : singleValues) {
                    val.add(((type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? quote(getValueForWhereClause(columnName, value, type).getValue()) : getValueForWhereClause(columnName, value, type).getValue()));
                }
                String values = Joiner.on(",").join(val);
                disjuncts.add(columnName + " IN (" + values + ")");
            }
        }

        return Joiner.on(" OR ").join(disjuncts);
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type)
    {
        return columnName + " " + operator + " " + ((type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? quote(getValueForWhereClause(columnName, value, type).getValue()) : getValueForWhereClause(columnName, value, type).getValue());
    }

    //Gets the representation of a value that can be used in a where clause, ie String values need to be quoted, numeric doesn't.
    private static QueryParameterValue getValueForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        LOGGER.info("Inside getValueForWhereClause(-, -, -): ");
        LOGGER.info("arrowType.getTypeID():" + arrowType.getTypeID());
        LOGGER.info("value:" + value);
        String val;
        StringBuilder tempVal;
        switch (arrowType.getTypeID()) {
            case Int:
                return QueryParameterValue.int64(((Number) value).longValue());
            case Decimal:
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return QueryParameterValue.numeric(BigDecimal.valueOf((long) value, decimalType.getScale()));
            case FloatingPoint:
                return QueryParameterValue.float64((double) value);
            case Bool:
                return QueryParameterValue.bool((Boolean) value);
            case Utf8:
                return QueryParameterValue.string(value.toString());
            case Date:
                val = value.toString();
                // Timestamp search: timestamp parameter in  where clause will come as string so it will be converted to date
                if
                (val.contains("-")) {
                    // Adding dot zero when parameter does not have micro seconds
                    tempVal = new StringBuilder(val);
                    tempVal = tempVal.length() == 19 ? tempVal.append(".0") : tempVal;

                    // Right side padding with required zeros
                    val = String.format("%-26s", tempVal).replace(' ', '0').replace("T", " ");
                    return QueryParameterValue.dateTime(val);
                }
                else {
                    // date search: date parameter used in where clause will come as days so it will be converted to date
                    long days = Long.parseLong(val);
                    long milliseconds = TimeUnit.DAYS.toMillis(days);
                    // convert date using UTC to avoid timezone conversion.
                    String dateString = Instant.ofEpochMilli(milliseconds)
                            .atOffset(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                    return QueryParameterValue.date(dateString);
                }
            case Time:
            case Timestamp:
            case Interval:
            case Binary:
            case FixedSizeBinary:
            case Null:
            case Struct:
            case List:
            case FixedSizeList:
            case Union:
            case NONE:
                throw new UnsupportedOperationException("The Arrow type: " + arrowType.getTypeID().name() + " is currently not supported");
            default:
                throw new IllegalArgumentException("Unknown type has been encountered during range processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }

    public static ReadSession.TableReadOptions.Builder setConstraints(ReadSession.TableReadOptions.Builder optionsBuilder, Schema schema, Constraints constraints)
    {
        List<String> clauses = toConjuncts(schema.getFields(), constraints);
        LOGGER.info("List of clause {}", clauses);
        if (!clauses.isEmpty()) {
            String clause = Joiner.on(" AND ").join(clauses);
            LOGGER.info("prepared clause value: {}", clause);
            optionsBuilder = optionsBuilder.setRowRestriction(clause);
        }
        return optionsBuilder;
    }
}
