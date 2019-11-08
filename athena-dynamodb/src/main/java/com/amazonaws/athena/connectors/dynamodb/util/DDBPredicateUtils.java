/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

public class DDBPredicateUtils
{
    private DDBPredicateUtils() {}

    private static final Joiner AND_JOINER = Joiner.on(" AND ");
    private static final Joiner COMMA_JOINER = Joiner.on(",");
    private static final Joiner OR_JOINER = Joiner.on(" OR ");

    public static DynamoDBTable getBestIndexForPredicates(DynamoDBTable table, Map<String, ValueSet> constraintSummary)
    {
        Set<String> columnNames = constraintSummary.keySet();

        ImmutableList.Builder<DynamoDBTable> hashKeyMatchesBuilder = ImmutableList.builder();
        // if the original table has a hash key matching a predicate, start with that
        if (columnNames.contains(table.getHashKey())) {
            hashKeyMatchesBuilder.add(table);
        }

        // get indices with hash keys that match a predicate
        table.getIndexes().stream()
                .filter(index -> columnNames.contains(index.getHashKey()) && !getHashKeyAttributeValues(constraintSummary.get(index.getHashKey())).isEmpty())
                .forEach(hashKeyMatchesBuilder::add);
        List<DynamoDBTable> hashKeyMatches = hashKeyMatchesBuilder.build();

        // if the original table has a range key matching a predicate, start with that
        ImmutableList.Builder<DynamoDBTable> rangeKeyMatchesBuilder = ImmutableList.builder();
        if (table.getRangeKey().isPresent() && columnNames.contains(table.getRangeKey().get())) {
            rangeKeyMatchesBuilder.add(table);
        }

        // get indices with range keys that match a predicate
        table.getIndexes().stream()
                .filter(index -> index.getRangeKey().isPresent() && columnNames.contains(index.getRangeKey().get()))
                .forEach(rangeKeyMatchesBuilder::add);
        List<DynamoDBTable> rangeKeyMatches = rangeKeyMatchesBuilder.build();

        // return first index where both hash and range key can be specified with predicates
        for (DynamoDBTable index : hashKeyMatches) {
            if (rangeKeyMatches.contains(index)) {
                return index;
            }
        }
        // else return the first index with a hash key predicate, or the original table if there are none
        return hashKeyMatches.isEmpty() ? table : hashKeyMatches.get(0);
    }

    public static List<Object> getHashKeyAttributeValues(ValueSet valueSet)
    {
        if (valueSet.isSingleValue()) {
            return ImmutableList.of(valueSet.getSingleValue());
        }
        else if (valueSet instanceof SortedRangeSet) {
            List<Range> ranges = valueSet.getRanges().getOrderedRanges();
            ImmutableList.Builder<Object> attributeValues = ImmutableList.builder();
            for (Range range : ranges) {
                if (range.isSingleValue()) {
                    attributeValues.add(range.getSingleValue());
                }
                else {
                    // DDB Query can't handle non-equality conditions for the hash key
                    return ImmutableList.of();
                }
            }
            return attributeValues.build();
        }
        else if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
            if (equatableValueSet.isWhiteList()) {
                ImmutableList.Builder<Object> values = ImmutableList.builder();
                for (int pos = 0; pos < equatableValueSet.getValueBlock().getRowCount(); pos++) {
                    values.add(equatableValueSet.getValue(pos));
                }
                return values.build();
            }
        }

        return ImmutableList.of();
    }

    public static String aliasColumn(String columnName)
    {
        return "#" + columnName;
    }

    private static void bindValue(Object value, List<AttributeValue> accumulator)
    {
        accumulator.add(ItemUtils.toAttributeValue(DDBTypeUtils.convertArrowTypeIfNecessary(value)));
    }

    private static String toPredicate(String columnName, String operator, Object value, List<AttributeValue> accumulator, String valueName)
    {
        bindValue(value, accumulator);
        return columnName + " " + operator + " " + valueName;
    }

    public static String generateSingleColumnFilter(String originalColumnName, ValueSet predicate, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer)
    {
        String columnName = aliasColumn(originalColumnName);

        if (predicate.isNone()) {
            return "(attribute_not_exists(" + columnName + ") OR " + toPredicate(columnName, "=", null, accumulator, valueNameProducer.getNext()) + ")";
        }

        if (predicate.isAll()) {
            return "(attribute_exists(" + columnName + ") AND " + toPredicate(columnName, "<>", null, accumulator, valueNameProducer.getNext()) + ")";
        }

        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        boolean isWhitelist = true;
        if (predicate instanceof SortedRangeSet) {
            for (Range range : predicate.getRanges().getOrderedRanges()) {
                checkState(!range.isAll()); // Already checked
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), accumulator, valueNameProducer.getNext()));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), accumulator, valueNameProducer.getNext()));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled lower bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), accumulator, valueNameProducer.getNext()));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), accumulator, valueNameProducer.getNext()));
                                break;
                            default:
                                throw new AssertionError("Unhandled upper bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add("(" + AND_JOINER.join(rangeConjuncts) + ")");
                }
            }
        }
        else {
            EquatableValueSet equatablePredicate = (EquatableValueSet) predicate;
            isWhitelist = equatablePredicate.isWhiteList();
            long valueCount = equatablePredicate.getValueBlock().getRowCount();
            for (int i = 0; i < valueCount; i++) {
                singleValues.add(equatablePredicate.getValue(i));
            }
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(toPredicate(columnName, isWhitelist ? "=" : "<>", getOnlyElement(singleValues), accumulator, valueNameProducer.getNext()));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(value, accumulator);
            }
            String values = COMMA_JOINER.join(Stream.generate(valueNameProducer::getNext).limit(singleValues.size()).collect(toImmutableList()));
            disjuncts.add((isWhitelist ? "" : "NOT ") + columnName + " IN (" + values + ")");
        }

        // at this point we should have some disjuncts
        checkState(!disjuncts.isEmpty());

        // add nullability disjuncts
        if (predicate.isNullAllowed()) {
            disjuncts.add("attribute_not_exists(" + columnName + ") OR " + toPredicate(columnName, "=", null, accumulator, valueNameProducer.getNext()));
        }

        // DDB doesn't like redundant parentheses
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }

        return "(" + OR_JOINER.join(disjuncts) + ")";
    }

    public static String generateFilterExpression(Set<String> columnsToIgnore, Map<String, ValueSet> predicates, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map.Entry<String, ValueSet> predicate : predicates.entrySet()) {
            String columnName = predicate.getKey();
            if (!columnsToIgnore.contains(columnName)) {
                builder.add(generateSingleColumnFilter(columnName, predicate.getValue(), accumulator, valueNameProducer));
            }
        }
        ImmutableList<String> filters = builder.build();
        if (!filters.isEmpty()) {
            return AND_JOINER.join(filters);
        }
        return null;
    }
}
