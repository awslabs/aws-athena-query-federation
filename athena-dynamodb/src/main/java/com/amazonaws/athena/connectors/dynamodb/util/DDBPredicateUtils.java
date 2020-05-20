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

/**
 * Provides utility methods relating to predicate handling.
 */
public class DDBPredicateUtils
{
    private DDBPredicateUtils() {}

    private static final Joiner AND_JOINER = Joiner.on(" AND ");
    private static final Joiner COMMA_JOINER = Joiner.on(",");
    private static final Joiner OR_JOINER = Joiner.on(" OR ");

    /**
     * Attempts to pick an optimal index (if any) from the given predicates. Returns the original table if
     * one was not found.
     *
     * @param table the original table
     * @param predicates the predicates
     * @return the optimal index if found, otherwise the original table
     */
    public static DynamoDBTable getBestIndexForPredicates(DynamoDBTable table, Map<String, ValueSet> predicates)
    {
        Set<String> columnNames = predicates.keySet();

        ImmutableList.Builder<DynamoDBTable> hashKeyMatchesBuilder = ImmutableList.builder();
        // if the original table has a hash key matching a predicate, start with that
        if (columnNames.contains(table.getHashKey())) {
            hashKeyMatchesBuilder.add(table);
        }

        // get indices with hash keys that match a predicate
        table.getIndexes().stream()
                .filter(index -> columnNames.contains(index.getHashKey()) && !getHashKeyAttributeValues(predicates.get(index.getHashKey())).isEmpty())
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

    /**
     * Generates a list of distinct values from the given {@link ValueSet} or an empty list if not possible.
     *
     * @param valueSet the value set to generate from
     * @return the list of distinct values
     */
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

    /**
     * Generates a simple alias for a column to satisfy filter expressions.
     *
     * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html">
     *     https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html</a>
     * @param columnName the input column name
     * @return the aliased column name
     */
    public static String aliasColumn(String columnName)
    {
        return "#" + columnName;
    }

    /*
    Adds a value to the value accumulator.
     */
    private static void bindValue(String columnName, Object value, List<AttributeValue> accumulator, DDBRecordMetadata recordMetadata)
    {
        accumulator.add(ItemUtils.toAttributeValue(DDBTypeUtils.convertArrowTypeIfNecessary(columnName, value, recordMetadata)));
    }

    /*
    Adds a value to the value accumulator and also returns an expression with given operands.
     */
    private static String toPredicate(String columnName, String operator, Object value, List<AttributeValue> accumulator,
                                      String valueName, DDBRecordMetadata recordMetadata)
    {
        bindValue(columnName, value, accumulator, recordMetadata);
        return aliasColumn(columnName) + " " + operator + " " + valueName;
    }

    /**
     * Generates a filter expression for a single column given a {@link ValueSet} predicate for that column.
     *
     * @param originalColumnName the column name
     * @param predicate the associated predicate
     * @param accumulator the value accumulator to add values to
     * @param valueNameProducer the value name producer to generate value aliases with
     * @param recordMetadata object containing any necessary metadata from the glue table
     * @return the generated filter expression
     */
    public static String generateSingleColumnFilter(String originalColumnName, ValueSet predicate, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer, DDBRecordMetadata recordMetadata)
    {
        String columnName = aliasColumn(originalColumnName);

        if (predicate.isNone()) {
            return "(attribute_not_exists(" + columnName + ") OR " + toPredicate(originalColumnName, "=", null, accumulator, valueNameProducer.getNext(), recordMetadata) + ")";
        }

        if (predicate.isAll()) {
            return "(attribute_exists(" + columnName + ") AND " + toPredicate(originalColumnName, "<>", null, accumulator, valueNameProducer.getNext(), recordMetadata) + ")";
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
                                rangeConjuncts.add(toPredicate(originalColumnName, ">", range.getLow().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(originalColumnName, ">=", range.getLow().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
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
                                rangeConjuncts.add(toPredicate(originalColumnName, "<=", range.getHigh().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(originalColumnName, "<", range.getHigh().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
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
            disjuncts.add(toPredicate(originalColumnName, isWhitelist ? "=" : "<>", getOnlyElement(singleValues), accumulator, valueNameProducer.getNext(), recordMetadata));
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(originalColumnName, value, accumulator, recordMetadata);
            }
            String values = COMMA_JOINER.join(Stream.generate(valueNameProducer::getNext).limit(singleValues.size()).collect(toImmutableList()));
            disjuncts.add((isWhitelist ? "" : "NOT ") + columnName + " IN (" + values + ")");
        }

        // at this point we should have some disjuncts
        checkState(!disjuncts.isEmpty());

        // add nullability disjuncts
        if (predicate.isNullAllowed()) {
            disjuncts.add("attribute_not_exists(" + columnName + ") OR " +
                    toPredicate(originalColumnName, "=", null, accumulator, valueNameProducer.getNext(), recordMetadata));
        }

        // DDB doesn't like redundant parentheses
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }

        return "(" + OR_JOINER.join(disjuncts) + ")";
    }

    /**
     * Generates a combined filter expression for the given predicates.
     *
     * columnsToIgnore will contain any column that is of custom type (such as timestamp with tz)
     * as these types are not natively supported by ddb or glue.
     * we will need to filter them separately in the ddb query/scan result.
     *
     * @param columnsToIgnore the columns to not generate filters for
     * @param predicates the map of columns to predicates
     * @param accumulator the value accumulator to add values to
     * @param valueNameProducer the value name producer to generate value aliases with
     * @param recordMetadata object containing any necessary metadata from the glue table
     * @return the combined filter expression
     */
    public static String generateFilterExpression(Set<String> columnsToIgnore, Map<String, ValueSet> predicates, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer, DDBRecordMetadata recordMetadata)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map.Entry<String, ValueSet> predicate : predicates.entrySet()) {
            String columnName = predicate.getKey();
            if (!columnsToIgnore.contains(columnName)) {
                builder.add(generateSingleColumnFilter(columnName, predicate.getValue(), accumulator, valueNameProducer, recordMetadata));
            }
        }
        ImmutableList<String> filters = builder.build();
        if (!filters.isEmpty()) {
            return AND_JOINER.join(filters);
        }
        return null;
    }
}
