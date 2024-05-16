/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PredicateBuilder
{
    // We use a specific format to use the full precision provided by Timestream.
    // Additionally, it prevents generating invalid format like `2024-12-31 00:11:22.`.
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")
            .toFormatter()
            .withZone(ZoneId.of("UTC"));

    private PredicateBuilder() {}

    public static List<String> buildConjucts(
            final Constraints constraints)
    {
        return toConjuncts(constraints);
    }

    private static List<String> toConjuncts(Constraints constraints)
    {
        List<String> conjuncts = new ArrayList<>();
        for (String column : constraints.getSummary().keySet()) {
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column);
                if (valueSet != null) {
                    conjuncts.add(toPredicate(column, valueSet));
                }
            }
        }
        return conjuncts;
    }

    private static String toPredicate(String columnName, ValueSet valueSet)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        // TODO Add isNone and isAll checks once we have data on nullability.

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
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), valueSet.getType()));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), valueSet.getType()));
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
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), valueSet.getType()));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), valueSet.getType()));
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
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), valueSet.getType()));
            }
            else if (singleValues.size() > 1) {
                List<String> values = singleValues.stream().map(next -> quoteValue(next, valueSet.getType())).collect(Collectors.toList());
                String valuesStr = Joiner.on(",").join(values);
                disjuncts.add(quoteColumn(columnName) + " IN (" + valuesStr + ")");
            }
        }
        else if (valueSet instanceof EquatableValueSet) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ((EquatableValueSet) valueSet).getValueBlock().getRowCount(); i++) {
                values.add(quoteValue(((EquatableValueSet) valueSet).getValue(i), valueSet.getType()));
            }
            String valuesStr = Joiner.on(",").join(values);
            disjuncts.add(quoteColumn(columnName) + " IN (" + valuesStr + ")");
        }

        return "(" + Joiner.on(" OR ").join(disjuncts) + ")";
    }

    private static String toPredicate(String columnName, String operator, Object value, ArrowType type)
    {
        return quoteColumn(columnName) + " " + operator + " " + quoteValue(value, type);
    }

    private static String quoteColumn(String name)
    {
        return "\"" + name + "\"";
    }

    private static String quoteValue(Object value, ArrowType type)
    {
        //TODO: add escaping
        switch (Types.getMinorTypeForArrowType(type)) {
            case VARCHAR:
                return "\'" + value + "\'";
            case DATEMILLI:
                return "\'" + ((LocalDateTime) value).format(TIMESTAMP_FORMATTER) + "\'";
            default:
                return String.valueOf(value);
        }
    }
}
