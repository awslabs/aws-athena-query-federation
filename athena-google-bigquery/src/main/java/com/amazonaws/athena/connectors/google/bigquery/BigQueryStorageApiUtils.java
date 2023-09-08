
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
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.storage.v1.ReadSession;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        LOGGER.debug("Inside toConjuncts(): ");
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Field column : columns) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    LOGGER.info("valueSet: ", valueSet);
                    builder.add(toPredicate(column.getName(), valueSet, type));
                }
            }
        }
        builder.addAll(new BigQueryFederationExpressionParser().parseComplexExpressions(columns, constraints));
        return builder.build();
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

    public static ReadSession.TableReadOptions.Builder setConstraints(ReadSession.TableReadOptions.Builder optionsBuilder, Schema schema, Constraints constraints)
    {
        List<String> clauses = toConjuncts(schema.getFields(), constraints);

        if (!clauses.isEmpty()) {
            String clause = Joiner.on(" AND ").join(clauses);
            LOGGER.debug("clause {}", clause);
            optionsBuilder = optionsBuilder.setRowRestriction(clause);
        }
        return optionsBuilder;
    }
}
