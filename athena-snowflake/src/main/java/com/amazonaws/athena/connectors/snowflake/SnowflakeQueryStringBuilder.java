/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Utf8;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements MySql specific SQL clauses for split.
 *
 * MySql provides named partitions which can be used in a FROM clause.
 */
public class SnowflakeQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeQueryStringBuilder.class);
    private static final String quoteCharacters = "\"";
    private static final String singleQuoteCharacters = "\'";

    public SnowflakeQueryStringBuilder(final String quoteCharacters, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));
        return String.format(" FROM %s ", tableName);
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        return Collections.emptyList();
    }

    public String buildSqlString(
            final Connection jdbcConnection,
            final String catalog,
            final String schema,
            final String table,
            final Schema tableSchema,
            final Constraints constraints,
            final Split split
    )
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(name -> !name.equalsIgnoreCase("partition"))
                .map(this::quote)
                .collect(Collectors.joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);

        if (columnNames.isEmpty()) {
            sql.append("null");
        }
        sql.append(getFromClauseWithSplit(catalog, schema, table, null));

        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> clauses = toConjuncts(tableSchema.getFields(), constraints, accumulator);
        clauses.addAll(getPartitionWhereClauses(null));
        if (!clauses.isEmpty()) {
            sql.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        String orderByClause = extractOrderByClause(constraints);

        if (!Strings.isNullOrEmpty(orderByClause)) {
            sql.append(" ").append(orderByClause);
        }

        if (constraints.getLimit() > 0) {
            sql.append(appendLimitOffset(null, constraints));
        }
        else {
            sql.append(appendLimitOffset(null));
        }
        LOGGER.info("Generated SQL : {}", sql.toString());
        return sql.toString();
    }

    protected String quote(String name)
    {
        name = name.replace(quoteCharacters, quoteCharacters + quoteCharacters);
        return quoteCharacters + name + quoteCharacters;
    }

    protected String singleQuote(String name)
    {
        name = name.replace(singleQuoteCharacters, singleQuoteCharacters + singleQuoteCharacters);
        return singleQuoteCharacters + name + singleQuoteCharacters;
    }

    private List<String> toConjuncts(List<Field> columns, Constraints constraints, List<TypeAndValue> accumulator)
    {
        List<String> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }
        return conjuncts;
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

            List<Range> rangeList = ((SortedRangeSet) valueSet).getOrderedRanges();
            if (rangeList.size() == 1 && !valueSet.isNullAllowed() && rangeList.get(0).getLow().isLowerUnbounded() && rangeList.get(0).getHigh().isUpperUnbounded()) {
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
                    disjuncts.add("(" + Joiner.on(" AND ").join(rangeConjuncts) + ")");
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type));
            }
            else if (singleValues.size() > 1) {
                List<Object> val = new ArrayList<>();
                for (Object value : singleValues) {
                    val.add(((type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? singleQuote(getObjectForWhereClause(columnName, value, type).toString()) : getObjectForWhereClause(columnName, value, type)));
                }
                String values = Joiner.on(",").join(val);
                disjuncts.add(columnName + " IN (" + values + ")");
            }
        }
        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    protected String toPredicate(String columnName, String operator, Object value, ArrowType type)
    {
        return columnName + " " + operator + " " + ((type.getTypeID().equals(Utf8) || type.getTypeID().equals(ArrowType.ArrowTypeID.Date)) ? singleQuote(getObjectForWhereClause(columnName, value, type).toString()) : getObjectForWhereClause(columnName, value, type));
    }

    protected static Object getObjectForWhereClause(String columnName, Object value, ArrowType arrowType)
    {
        String val;
        StringBuilder tempVal;

        switch (arrowType.getTypeID()) {
            case Int:
                return ((Number) value).longValue();
            case Decimal:
                if (value instanceof BigDecimal) {
                    return (BigDecimal) value;
                }
                else if (value instanceof Number) {
                    return BigDecimal.valueOf(((Number) value).doubleValue());
                }
                else {
                    throw new IllegalArgumentException("Unexpected type for decimal conversion: " + value.getClass().getName());
                }
            case FloatingPoint:
                return (double) value;
            case Bool:
                return (Boolean) value;
            case Utf8:
                return value.toString();
            case Date:
                String dateStr = value.toString();
                if (dateStr.contains("-") && dateStr.length() == 16) {
                    LocalDateTime dateTime = LocalDateTime.parse(dateStr);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    return dateTime.format(formatter);
                }
                else {
                    long days = Long.parseLong(dateStr);
                    long milliseconds = TimeUnit.DAYS.toMillis(days);
                    return new SimpleDateFormat("yyyy-MM-dd").format(new Date(milliseconds));
                }
            case Timestamp:
                long millis = ((Number) value).longValue();
                Timestamp timestamp = new Timestamp(millis);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return sdf.format(timestamp);
            case Time:
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
                throw new IllegalArgumentException("Unknown type encountered during processing: " + columnName +
                        " Field Type: " + arrowType.getTypeID().name());
        }
    }
}
