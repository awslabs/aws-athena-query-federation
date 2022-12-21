/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.FederationExpressionParser;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.OperatorType;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.functions.StandardFunctions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Query builder for database table split.
 */
public abstract class JdbcSplitQueryBuilder extends FederationExpressionParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSplitQueryBuilder.class);

    private static final int MILLIS_SHIFT = 12;

    private final String quoteCharacters;
    private final String emptyString = "";

    /**
     * @param quoteCharacters database quote character for enclosing identifiers.
     */
    public JdbcSplitQueryBuilder(String quoteCharacters)
    {
        this.quoteCharacters = quoteCharacters;
    }

    /**
     * Common logic to build Split SQL including constraints translated in where clause.
     *
     * @param jdbcConnection JDBC connection. See {@link Connection}.
     * @param catalog Athena provided catalog name.
     * @param schema table schema name.
     * @param table table name.
     * @param tableSchema table schema (column and type information).
     * @param constraints constraints passed by Athena to push down.
     * @param split table split.
     * @return prepated statement with SQL. See {@link PreparedStatement}.
     * @throws SQLException JDBC database exception.
     */
    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    public PreparedStatement buildSql(
            final Connection jdbcConnection,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columnNames.isEmpty()) {
            sql.append("null");
        }
        sql.append(getFromClauseWithSplit(catalog, schema, table, split));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(tableSchema.getFields(), constraints, accumulator, split.getProperties());
        clauses.addAll(getPartitionWhereClauses(split));
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }
        if (constraints.getLimit() > 0) {
            sql.append(appendLimitOffset(split, constraints));
        }
        else {
            sql.append(appendLimitOffset(split)); // legacy method to preserve functionality of existing connector impls
        }
        LOGGER.debug("Generated SQL : {}", sql.toString());
        PreparedStatement statement = jdbcConnection.prepareStatement(sql.toString());

        // TODO all types, converts Arrow values to JDBC.
        for (int i = 0; i < accumulator.size(); i++) {
            TypeAndValue typeAndValue = accumulator.get(i);

            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());

            switch (minorTypeForArrowType) {
                case BIGINT:
                    statement.setLong(i + 1, (long) typeAndValue.getValue());
                    break;
                case INT:
                    statement.setInt(i + 1, ((Number) typeAndValue.getValue()).intValue());
                    break;
                case SMALLINT:
                    statement.setShort(i + 1, ((Number) typeAndValue.getValue()).shortValue());
                    break;
                case TINYINT:
                    statement.setByte(i + 1, ((Number) typeAndValue.getValue()).byteValue());
                    break;
                case FLOAT8:
                    statement.setDouble(i + 1, (double) typeAndValue.getValue());
                    break;
                case FLOAT4:
                    statement.setFloat(i + 1, (float) typeAndValue.getValue());
                    break;
                case BIT:
                    statement.setBoolean(i + 1, (boolean) typeAndValue.getValue());
                    break;
                case DATEDAY:
                    statement.setDate(i + 1,
                            new Date(TimeUnit.DAYS.toMillis(((Number) typeAndValue.getValue()).longValue())));
                    break;
                case DATEMILLI:
                    LocalDateTime timestamp = ((LocalDateTime) typeAndValue.getValue());
                    statement.setTimestamp(i + 1, new Timestamp(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()));
                    break;
                case VARCHAR:
                    statement.setString(i + 1, String.valueOf(typeAndValue.getValue()));
                    break;
                case VARBINARY:
                    statement.setBytes(i + 1, (byte[]) typeAndValue.getValue());
                    break;
                case DECIMAL:
                    statement.setBigDecimal(i + 1, (BigDecimal) typeAndValue.getValue());
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType));
            }
        }

        return statement;
    }

    protected abstract String getFromClauseWithSplit(final String catalog, final String schema, final String table, final Split split);

    protected abstract List<String> getPartitionWhereClauses(final Split split);

    private List<String> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator, Map<String, String> partitionSplit)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }
        conjuncts.addAll(parseComplexExpressions(columns, constraints, quoteCharacters)); // not part of loop bc not per-column
        return conjuncts;
    }

    // this method is not abstract so as to not break other subclasses that don't support complex expressions at this time.
    public String writeCastClause(ArrowType type, List<String> arguments)
    {
        throw new NotImplementedException("CAST operation not implemented. Implement this method in the subclass extending this class to use CAST.");
    }

    @Override
    public String mapFunctionToDataSourceSyntax(FunctionName functionName, ArrowType type, List<String> arguments)
    {
        StandardFunctions functionEnum = StandardFunctions.fromFunctionName(functionName);
        OperatorType operatorType = functionEnum.getOperatorType();

        if (arguments == null || arguments.size() == 0) {
            throw new IllegalArgumentException("Arguments cannot be null or empty.");
        }
        switch (operatorType) {
            case UNARY:
                if (arguments.size() != 1) {
                    throw new IllegalArgumentException("Unary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case BINARY:
                if (arguments.size() != 2) {
                    throw new IllegalArgumentException("Binary function type " + functionName.getFunctionName() + " was provided with " + arguments.size() + " arguments.");
                }
                break;
            case VARARG:
                break;
            default:
                throw new RuntimeException("A new operator type was introduced without adding support for it.");
        }

        String clause = "";
        switch (functionEnum) {
            case ADD_FUNCTION_NAME:
                clause = Joiner.on(" + ").join(arguments);
                break;
            case AND_FUNCTION_NAME:
                clause = Joiner.on(" AND ").join(arguments);
                break;
            case ARRAY_CONSTRUCTOR_FUNCTION_NAME:
                throw new NotImplementedException("ARRAY CONSTRUCTOR FUNCTION NOT YET SUPPORTED");
            case CAST_FUNCTION_NAME:
                clause = writeCastClause(type, arguments);
                break;
            case DIVIDE_FUNCTION_NAME:
                clause = Joiner.on(" / ").join(arguments);
                break;
            case EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" = ").join(arguments);
                break;
            case GREATER_THAN_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" > ").join(arguments);
                break;
            case GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" >= ").join(arguments);
                break;
            case IN_PREDICATE_FUNCTION_NAME:
                clause = arguments.get(0) + " IN (" + arguments.get(1) + ")";
                break;
            case IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME:
                String argZero = arguments.get(0);
                String argOne = arguments.get(1);
                clause = argZero + " IS DISTINCT FROM " + argOne;
                break;
            case IS_NULL_FUNCTION_NAME:
                clause = arguments.get(0) + " IS NULL";
                break;
            case LESS_THAN_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" < ").join(arguments);
                break;
            case LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" <= ").join(arguments);
                break;
            case LIKE_PATTERN_FUNCTION_NAME:
                clause = arguments.get(0) + " LIKE " + quoteCharacters + arguments.get(1) + quoteCharacters;
                break;
            case MODULUS_FUNCTION_NAME:
                clause = Joiner.on(" % ").join(arguments);
                break;
            case MULTIPLY_FUNCTION_NAME:
                clause = Joiner.on(" * ").join(arguments);
                break;
            case NEGATE_FUNCTION_NAME:
                clause = "-" + arguments.get(0);
                break;
            case NOT_EQUAL_OPERATOR_FUNCTION_NAME:
                clause = Joiner.on(" <> ").join(arguments);
                break;
            case NOT_FUNCTION_NAME:
                clause = Joiner.on(" IS NOT ").join(arguments);
                break;
            case NULLIF_FUNCTION_NAME:
                clause = "NULLIF(" + arguments.get(0) + ", " + arguments.get(1) + ")";
                break;
            case OR_FUNCTION_NAME:
                clause = Joiner.on(" OR ").join(arguments);
                break;
            case SUBTRACT_FUNCTION_NAME:
                clause = Joiner.on(" - ").join(arguments);
                break;
            default:
                throw new NotImplementedException("The function " + functionName.getFunctionName() + " does not have an implementation");
        }
        if (clause == null) {
          return emptyString;
        }
        return "(" + clause + ")";
    }

    private String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    accumulator.add(new TypeAndValue(type, value));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "?"));
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private String toPredicate(String columnName, String operator, Object value, ArrowType type,
            List<TypeAndValue> accumulator)
    {
        accumulator.add(new TypeAndValue(type, value));
        return quote(columnName) + " " + operator + " ?";
    }

    protected String quote(String name)
    {
        name = name.replace(quoteCharacters, quoteCharacters + quoteCharacters);
        return quoteCharacters + name + quoteCharacters;
    }

    private static class TypeAndValue
    {
        private final ArrowType type;
        private final Object value;

        TypeAndValue(ArrowType type, Object value)
        {
            this.type = Validate.notNull(type, "type is null");
            this.value = Validate.notNull(value, "value is null");
        }

        ArrowType getType()
        {
            return type;
        }

        Object getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return "TypeAndValue{" +
                    "type=" + type +
                    ", value=" + value +
                    '}';
        }
    }

    protected String appendLimitOffset(Split split)
    {
        // keeping this method for connectors that still override this (SAP Hana + Snowflake)
        return emptyString;
    }

    protected String appendLimitOffset(Split split, Constraints constraints)
    {
        return " LIMIT " + constraints.getLimit();
    }
}
