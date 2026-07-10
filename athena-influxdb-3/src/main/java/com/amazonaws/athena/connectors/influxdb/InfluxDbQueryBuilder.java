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

import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
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
import java.time.ZoneOffset;
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
        return buildSql(schema, tableName, constraints, null, null);
    }

    /**
     * Builds a complete SQL query, additionally narrowing the query to a single
     * time-based split via a half-open {@code [lower, upper)} predicate on the
     * time column. The bounds are epoch-millisecond strings taken from the split
     * properties; when either is null no time-based narrowing is applied.
     */
    public static String buildSql(final Schema schema, final String tableName, final Constraints constraints,
            final String timeLowerMillis, final String timeUpperMillis)
    {
        final StringBuilder sql = new StringBuilder();

        // SELECT columns
        final String columns = schema.getFields().stream()
                .map(f -> quote(f.getName()))
                .collect(Collectors.joining(", "));
        sql.append("SELECT ").append(columns).append(" FROM ").append(quote(tableName));

        // WHERE clause from constraints, plus the optional per-split time bound
        final List<String> conjuncts = new ArrayList<>();
        final String whereClause = buildWhereClause(schema, constraints);
        if (!whereClause.isEmpty()) {
            conjuncts.add(whereClause);
        }
        final String splitPredicate = buildSplitTimePredicate(timeLowerMillis, timeUpperMillis);
        if (!splitPredicate.isEmpty()) {
            conjuncts.add(splitPredicate);
        }
        if (!conjuncts.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conjuncts));
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
     * Builds a half-open {@code [lower, upper)} predicate on the time column from
     * epoch-millisecond bounds. Returns an empty string when either bound is null.
     */
    static String buildSplitTimePredicate(final String timeLowerMillis, final String timeUpperMillis)
    {
        if (timeLowerMillis == null || timeUpperMillis == null) {
            return "";
        }
        final long low = Long.parseLong(timeLowerMillis);
        final long high = Long.parseLong(timeUpperMillis);
        final String col = quote(InfluxDbConstants.DEFAULT_TIME_COLUMN);
        // Split bounds are already plain epoch millis (decoded in getPartitions),
        // so format them directly rather than through the packing-aware path.
        return "(" + col + " >= " + timestampLiteral(low)
                + " AND " + col + " < " + timestampLiteral(high) + ")";
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
                            rangeConjuncts.add(quote(columnName) + " > " + constraintLiteral(range.getLow().getValue(), type));
                            break;
                        case EXACTLY :
                            rangeConjuncts.add(quote(columnName) + " >= " + constraintLiteral(range.getLow().getValue(), type));
                            break;
                        default :
                            break;
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY :
                            rangeConjuncts
                                    .add(quote(columnName) + " <= " + constraintLiteral(range.getHigh().getValue(), type));
                            break;
                        case BELOW :
                            rangeConjuncts.add(quote(columnName) + " < " + constraintLiteral(range.getHigh().getValue(), type));
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
            disjuncts.add(quote(columnName) + " = " + constraintLiteral(singleValues.get(0), type));
        }
        else if (singleValues.size() > 1) {
            final String values = singleValues.stream()
                    .map(v -> constraintLiteral(v, type))
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
        return constraintLiteral(value, expr.getType());
    }

    /**
     * Decodes an Athena TIMESTAMP-with-time-zone constraint value to epoch millis.
     * Athena (Trino engine) packs such values as {@code (millisUtc << 12) | timeZoneKey};
     * the SDK's {@link DateTimeFormatterUtil#constructZonedDateTime} unpacks that. Other
     * temporal representations are handled directly for robustness.
     */
    static long constraintEpochMillis(final Object value, final ArrowType.Timestamp arrowType)
    {
        if (value instanceof Number) {
            return DateTimeFormatterUtil.constructZonedDateTime(((Number) value).longValue(), arrowType)
                    .toInstant().toEpochMilli();
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toInstant().toEpochMilli();
        }
        if (value instanceof Instant) {
            return ((Instant) value).toEpochMilli();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        return Instant.parse(String.valueOf(value)).toEpochMilli();
    }

    /**
     * Renders a plain epoch-millisecond value as a SQL {@code TIMESTAMP} literal
     * (UTC, millisecond precision).
     */
    static String timestampLiteral(final long epochMillis)
    {
        return "TIMESTAMP '" + DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(epochMillis)) + "'";
    }

    /**
     * Renders a constraint value (as delivered by Athena) as a SQL literal. Athena
     * packs TIMESTAMP-with-time-zone values (millisUtc &lt;&lt; 12 | tzKey), so those
     * are decoded here before formatting; all other types delegate to {@link #toLiteral}.
     */
    static String constraintLiteral(final Object value, final ArrowType type)
    {
        if (value != null
                && Types.getMinorTypeForArrowType(type) == Types.MinorType.TIMESTAMPMILLITZ
                && type instanceof ArrowType.Timestamp) {
            return timestampLiteral(constraintEpochMillis(value, (ArrowType.Timestamp) type));
        }
        return toLiteral(value, type);
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
                // Plain epoch-millis (or temporal) value to literal. Packed Athena
                // constraint values are decoded upstream by constraintLiteral, so this
                // must NOT unpack. ISO_INSTANT keeps millisecond precision (trailing 'Z').
                final Instant instant;
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
