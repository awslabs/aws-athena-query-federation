/*-
 * #%L
 * athena-influxdb
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
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.VariableExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * Builds SQL queries for InfluxDB 3 from Athena SDK constraints. Since InfluxDB speaks SQL natively, we construct standard SQL strings directly.
 */
public class InfluxDbQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbQueryBuilder.class);

    private InfluxDbQueryBuilder()
    {
    }

    /**
     * Builds a complete SQL query from the schema, table name, and constraints.
     */
    public static String buildSql(final Schema schema, final String tableName, final Constraints constraints)
    {
        final StringBuilder sql = new StringBuilder();

        // SELECT columns
        final String columns = schema.getFields().stream()
                .map(f -> quote(f.getName()))
                .collect(Collectors.joining(", "));
        sql.append("SELECT ").append(columns).append(" FROM ").append(quote(tableName));

        // WHERE clause from constraints
        final String whereClause = buildWhereClause(schema, constraints);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }

        // ORDER BY clause
        if (constraints.hasNonEmptyOrderByClause()) {
            sql.append(" ").append(buildOrderByClause(constraints));
        }

        // LIMIT clause
        if (constraints.hasLimit()) {
            sql.append(" LIMIT ").append(constraints.getLimit());
        }

        logger.info("buildSql: {}", sql);
        return sql.toString();
    }

    /**
     * Builds a WHERE clause from the constraints summary (ValueSets) and complex expressions.
     */
    static String buildWhereClause(final Schema schema, final Constraints constraints)
    {
        final List<String> conjuncts = new ArrayList<>();

        // Process ValueSet-based constraints (summary)
        if (constraints.getSummary() != null) {
            for (final Field field : schema.getFields()) {
                final ValueSet valueSet = constraints.getSummary().get(field.getName());
                if (valueSet != null) {
                    final String predicate = toPredicate(field.getName(), valueSet, field.getType());
                    if (predicate != null && !predicate.isEmpty()) {
                        conjuncts.add(predicate);
                    }
                }
            }
        }

        // Process complex expressions
        if (constraints.getExpression() != null && !constraints.getExpression().isEmpty()) {
            for (final FederationExpression expr : constraints.getExpression()) {
                if (expr instanceof FunctionCallExpression) {
                    final String exprSql = toExpression((FunctionCallExpression) expr);
                    if (exprSql != null && !exprSql.isEmpty()) {
                        conjuncts.add(exprSql);
                    }
                }
            }
        }

        return String.join(" AND ", conjuncts);
    }

    static String buildOrderByClause(final Constraints constraints)
    {
        final List<OrderByField> orderBy = constraints.getOrderByClause();
        if (orderBy == null || orderBy.isEmpty()) {
            return "";
        }
        final String fields = orderBy.stream()
                .map(f -> quote(f.getColumnName()) + " " +
                        (f.getDirection().isAscending() ? "ASC" : "DESC") + " " +
                        (f.getDirection().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST"))
                .collect(Collectors.joining(", "));
        return "ORDER BY " + fields;
    }

    /**
     * Converts a ValueSet into a SQL predicate for a single column.
     */
    private static String toPredicate(final String columnName, final ValueSet valueSet, final ArrowType type)
    {
        if (!(valueSet instanceof SortedRangeSet)) {
            return null;
        }

        final List<String> disjuncts = new ArrayList<>();
        final List<Object> singleValues = new ArrayList<>();

        if (valueSet.isNone() && valueSet.isNullAllowed()) {
            return "(" + quote(columnName) + " IS NULL)";
        }

        if (valueSet.isNullAllowed()) {
            disjuncts.add("(" + quote(columnName) + " IS NULL)");
        }

        final List<Range> rangeList = ((SortedRangeSet) valueSet).getOrderedRanges();
        if (rangeList.size() == 1 && !valueSet.isNullAllowed()
                && rangeList.get(0).getLow().isLowerUnbounded()
                && rangeList.get(0).getHigh().isUpperUnbounded()) {
            return "(" + quote(columnName) + " IS NOT NULL)";
        }

        for (final Range range : rangeList) {
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            }
            else {
                final List<String> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE :
                            rangeConjuncts.add(quote(columnName) + " > " + toLiteral(range.getLow().getValue(), type));
                            break;
                        case EXACTLY :
                            rangeConjuncts.add(quote(columnName) + " >= " + toLiteral(range.getLow().getValue(), type));
                            break;
                        default :
                            break;
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY :
                            rangeConjuncts
                                    .add(quote(columnName) + " <= " + toLiteral(range.getHigh().getValue(), type));
                            break;
                        case BELOW :
                            rangeConjuncts.add(quote(columnName) + " < " + toLiteral(range.getHigh().getValue(), type));
                            break;
                        default :
                            break;
                    }
                }
                if (!rangeConjuncts.isEmpty()) {
                    disjuncts.add("(" + String.join(" AND ", rangeConjuncts) + ")");
                }
            }
        }

        // Single values as equality or IN
        if (singleValues.size() == 1) {
            disjuncts.add(quote(columnName) + " = " + toLiteral(singleValues.get(0), type));
        }
        else if (singleValues.size() > 1) {
            final String values = singleValues.stream()
                    .map(v -> toLiteral(v, type))
                    .collect(Collectors.joining(", "));
            disjuncts.add(quote(columnName) + " IN (" + values + ")");
        }

        if (disjuncts.isEmpty()) {
            return null;
        }
        return "(" + String.join(" OR ", disjuncts) + ")";
    }

    /**
     * Recursively converts a FunctionCallExpression into SQL.
     */
    private static String toExpression(final FunctionCallExpression expr)
    {
        final FunctionName functionName = expr.getFunctionName();
        final StandardFunctions func = StandardFunctions.fromFunctionName(functionName);
        final List<FederationExpression> args = expr.getArguments();

        final List<String> argStrings = args.stream().map(arg -> {
            if (arg instanceof VariableExpression) {
                return quote(((VariableExpression) arg).getColumnName());
            }
            else if (arg instanceof ConstantExpression) {
                return constantToLiteral((ConstantExpression) arg);
            }
            else if (arg instanceof FunctionCallExpression) {
                return toExpression((FunctionCallExpression) arg);
            }
            return "NULL";
        }).collect(Collectors.toList());

        switch (func) {
            case AND_FUNCTION_NAME :
                return "(" + String.join(" AND ", argStrings) + ")";
            case OR_FUNCTION_NAME :
                return "(" + String.join(" OR ", argStrings) + ")";
            case NOT_FUNCTION_NAME :
                return "(NOT " + argStrings.get(0) + ")";
            case IS_NULL_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " IS NULL)";
            case NULLIF_FUNCTION_NAME :
                return "NULLIF(" + argStrings.get(0) + ", " + argStrings.get(1) + ")";
            case EQUAL_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " = " + argStrings.get(1) + ")";
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " <> " + argStrings.get(1) + ")";
            case LESS_THAN_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " < " + argStrings.get(1) + ")";
            case GREATER_THAN_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " > " + argStrings.get(1) + ")";
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " <= " + argStrings.get(1) + ")";
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " >= " + argStrings.get(1) + ")";
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " IS DISTINCT FROM " + argStrings.get(1) + ")";
            case LIKE_PATTERN_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " LIKE " + argStrings.get(1) + ")";
            case IN_PREDICATE_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " IN (" + argStrings.get(1) + "))";
            case ADD_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " + " + argStrings.get(1) + ")";
            case SUBTRACT_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " - " + argStrings.get(1) + ")";
            case MULTIPLY_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " * " + argStrings.get(1) + ")";
            case DIVIDE_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " / " + argStrings.get(1) + ")";
            case MODULUS_FUNCTION_NAME :
                return "(" + argStrings.get(0) + " % " + argStrings.get(1) + ")";
            case NEGATE_FUNCTION_NAME :
                return "(-" + argStrings.get(0) + ")";
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME :
                return String.join(", ", argStrings);
            default :
                logger.warn("Unsupported function in expression pushdown: {}", functionName.getFunctionName());
                return null;
        }
    }

    /**
     * Converts a ConstantExpression to a SQL literal string.
     */
    private static String constantToLiteral(final ConstantExpression expr)
    {
        final FieldReader reader = expr.getValues().getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);
        if (expr.getValues().getRowCount() == 0) {
            return "NULL";
        }
        reader.setPosition(0);
        final Object value = reader.readObject();
        return toLiteral(value, expr.getType());
    }

    /**
     * Converts a Java value to a SQL literal based on its Arrow type.
     */
    static String toLiteral(final Object value, final ArrowType type)
    {
        if (value == null) {
            return "NULL";
        }

        final Types.MinorType minorType = Types.getMinorTypeForArrowType(type);
        switch (minorType) {
            case VARCHAR :
                // Escape single quotes
                return "'" + String.valueOf(value).replace("'", "''") + "'";
            case BIT :
                return Boolean.TRUE.equals(value) ? "true" : "false";
            case TIMESTAMPMILLITZ : {
                // Athena delivers TIMESTAMPMILLITZ predicate values as epoch millis (Long).
                // ZonedDateTime/Instant is also possible. Normalize to UTC.
                Instant instant;
                if (value instanceof Number) {
                    instant = Instant.ofEpochMilli(((Number) value).longValue());
                }
                else if (value instanceof ZonedDateTime) {
                    instant = ((ZonedDateTime) value).toInstant();
                }
                else if (value instanceof Instant) {
                    instant = (Instant) value;
                }
                else {
                    instant = Instant.parse(String.valueOf(value));
                }
                // ISO_INSTANT keeps millisecond precision with a trailing 'Z'.
                // For example, '2025-12-03T10:15:30.123Z'.
                return "TIMESTAMP '" + DateTimeFormatter.ISO_INSTANT.format(instant) + "'";
            }
            case DATEMILLI : {
                // Convert epoch millis or LocalDateTime to timestamp literal
                if (value instanceof LocalDateTime) {
                    return "'" + DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((LocalDateTime) value) + "'";
                }
                if (value instanceof Number) {
                    final Instant instant = Instant.ofEpochMilli(((Number) value).longValue());
                    return "'" + DateTimeFormatter.ISO_INSTANT.format(instant) + "'";
                }
                return "'" + value + "'";
            }
            case BIGINT :
            case INT :
            case FLOAT8 :
            case FLOAT4 :
                return String.valueOf(value);
            default :
                return "'" + String.valueOf(value).replace("'", "''") + "'";
        }
    }

    static String quote(final String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
