
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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

/**
 * Utilities that help with Sql operations.
 */
public class BigQuerySqlUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryMetadataHandler.class);

    private static final String BIGQUERY_QUOTE_CHAR = "`";

    private BigQuerySqlUtils()
    {
    }

    /**
     * Builds an SQL statement from the schema, table name, split and contraints that can be executable by
     * BigQuery.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information to add as a constraint.
     * @param parameterValues Query parameter values for parameterized query.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    public static String buildSqlFromSplit(TableName tableName, Schema schema, Constraints constraints, Split split, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("Inside buildSqlFromSplit(): ");
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");
        Map<String, String> limitAndOffsets = split.getProperties();
        StringJoiner sj = new StringJoiner(",");
        if (schema.getFields().isEmpty()) {
            sj.add("null");
        }
        else {
            for (Field field : schema.getFields()) {
                sj.add(quote(field.getName()));
            }
        }
        sqlBuilder.append(sj.toString())
            .append(" from ")
            .append(quote(tableName.getSchemaName()))
            .append(".")
            .append(quote(tableName.getTableName()));

        LOGGER.info("constraints: " + constraints);
        List<String> clauses = toConjuncts(schema.getFields(), constraints, split.getProperties(), parameterValues);

        if (!clauses.isEmpty()) {
            sqlBuilder.append(" WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        if (limitAndOffsets.size() > 0) {
            for (Map.Entry<String, String> entry : limitAndOffsets.entrySet()) {
                LOGGER.info("entry.getValue())" + entry.getValue());
                LOGGER.info("entry.getKey()" + entry.getKey());
                sqlBuilder.append(" limit " + entry.getKey() + " offset " + entry.getValue());
            }
        }

        return sqlBuilder.toString();
    }

    private static String quote(final String identifier)
    {
        return BIGQUERY_QUOTE_CHAR + identifier + BIGQUERY_QUOTE_CHAR;
    }

    private static List<String> toConjuncts(List<Field> columns, Constraints constraints, Map<String, String> partitionSplit, List<QueryParameterValue> parameterValues)
    {
        LOGGER.info("Inside toConjuncts(): ");
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue; // Ignore constraints on partition name as RDBMS does not contain these as columns. Presto will filter these values.
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.info("valueSet: ", valueSet);
                    builder.add(toPredicate(column.getName(), valueSet, type, parameterValues));
                }
            }
        }
        return builder.build();
    }

    private static String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<QueryParameterValue> parameterValues)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return String.format("(%s IS NULL)", columnName);
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(String.format("(%s IS NULL)", columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return String.format("(%s IS NOT NULL)", columnName);
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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, parameterValues));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, parameterValues));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, parameterValues));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, parameterValues));
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
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, parameterValues));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    parameterValues.add(getValueForWhereClause(columnName, value, type));
                }
                String values = Joiner.on(",").join(Collections.nCopies(singleValues.size(), "?"));
                disjuncts.add(quote(columnName) + " IN (" + values + ")");
            }
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type,
            List<QueryParameterValue> parameterValues)
    {
        parameterValues.add(getValueForWhereClause(columnName, value, type));
        return quote(columnName) + " " + operator + " ?";
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
                    return QueryParameterValue.date(new SimpleDateFormat("yyyy-MM-dd").format(new Date(milliseconds)));
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
}
