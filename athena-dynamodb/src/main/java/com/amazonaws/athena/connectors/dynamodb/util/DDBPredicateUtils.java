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
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitFunctionParser;
import com.amazonaws.athena.connector.substrait.SubstraitMetadataParser;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.amazonaws.athena.connector.substrait.model.SubstraitOperator;
import com.amazonaws.athena.connector.substrait.model.SubstraitRelModel;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBIndex;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.substrait.proto.Plan;
import io.substrait.proto.SimpleExtensionDeclaration;
import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
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
     * Attempts to pick an optimal index (if any) from the given predicates. Returns the original table index if
     * one was not found.
     *
     * @param table the original table
     * @param predicates the predicates
     * @return the optimal index if found, otherwise the original table index
     */
    public static DynamoDBIndex getBestIndexForPredicates(DynamoDBTable table, List<String> requestedCols, Map<String, ValueSet> predicates)
    {
        Set<String> columnNames = predicates.keySet();
        return findBestIndexInternal(table, requestedCols,
                columnNames::contains,
            columnName -> !getHashKeyAttributeValues(predicates.get(columnName)).isEmpty());
    }

    public static DynamoDBIndex getBestIndexForPredicatesForPlan(DynamoDBTable table,
                                                                 List<String> requestedCols,
                                                                 Map<String, List<ColumnPredicate>> filterPredicates)
    {
        if (filterPredicates == null || filterPredicates.isEmpty()) {
            return createTableIndex(table);
        }
        return findBestIndexForPlan(table, requestedCols, filterPredicates);
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
     * Generates a simple alias for a column to satisfy filter expressions. Uses a regex to convert illegal characters
     * (any character or combination of characters that are NOT included in [a-zA-Z_0-9]) to underscore.
     * Example: "column-$1`~!@#$%^&*()-=+[]{}\\|;:'\",.<>/?f3" -> "#column_1_f3"
     *
     * @see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html">
     *     https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html</a>
     * @param columnName the input column name
     * @return the aliased column name
     */
    public static String aliasColumn(String columnName)
    {
        return "#" + columnName.replaceAll("\\W+", "_");
    }

    /**
     * Builds filter predicates from a Substrait execution plan.
     * 
     * @param plan the Substrait plan containing filter conditions
     * @return map of column names to their predicates
     */
    public static Map<String, List<ColumnPredicate>> buildFilterPredicatesFromPlan(Plan plan)
    {
        if (plan == null || plan.getRelationsList().isEmpty()) {
            return new HashMap<>();
        }
        
        SubstraitRelModel substraitRelModel = extractSubstraitRelModel(plan);
        if (substraitRelModel.getFilterRel() == null) {
            return new HashMap<>();
        }
        
        List<SimpleExtensionDeclaration> extensionDeclarations = plan.getExtensionsList();
        List<String> tableColumns = SubstraitMetadataParser.getTableColumns(substraitRelModel);
        
        return SubstraitFunctionParser.getColumnPredicatesMap(
            extensionDeclarations, substraitRelModel.getFilterRel().getCondition(), tableColumns);
    }
    
    /**
     * Extracts the Substrait relation model from the plan.
     */
    private static SubstraitRelModel extractSubstraitRelModel(Plan plan)
    {
        // Considering only one table is used without joins
        return SubstraitRelModel.buildSubstraitRelModel(plan.getRelations(0).getRoot().getInput());
    }

    public static List<ColumnPredicate> getHashKeyAttributeValues(List<ColumnPredicate> columnPredicates)
    {
        if (columnPredicates == null || columnPredicates.isEmpty()) {
            return ImmutableList.of();
        }
        return columnPredicates.stream()
                .filter(columnPredicate -> SubstraitOperator.EQUAL.equals(columnPredicate.getOperator()))
                .collect(Collectors.toList());
    }

    private static DynamoDBIndex findBestIndexForPlan(DynamoDBTable table,
                                                      List<String> requestedCols,
                                                      Map<String, List<ColumnPredicate>> columnPredicatesMap)
    {
        return findBestIndexInternal(table, requestedCols,
                columnPredicatesMap::containsKey,
            columnName -> !getHashKeyAttributeValues(columnPredicatesMap.get(columnName)).isEmpty());
    }

    private static DynamoDBIndex createTableIndex(DynamoDBTable table)
    {
        return new DynamoDBIndex(table.getName(), table.getHashKey(), table.getRangeKey(), ProjectionType.ALL, ImmutableList.of());
    }

    private static DynamoDBIndex findBestIndexInternal(DynamoDBTable table,
                                                      List<String> requestedCols,
                                                      Predicate<String> hasPredicateForColumn,
                                                      Predicate<String> hasValidHashKeyValues)
    {
        DynamoDBIndex tableIndex = createTableIndex(table);

        ImmutableList.Builder<DynamoDBIndex> hashKeyMatchesBuilder = ImmutableList.builder();

        // if the original table has a hash key matching a predicate, start with that
        if (hasPredicateForColumn.test(tableIndex.getHashKey())) {
            hashKeyMatchesBuilder.add(tableIndex);
        }

        // requested columns must be projected in index
        List<DynamoDBIndex> candidateIndices = table.getIndexes().stream()
                .filter(index -> indexContainsAllRequiredColumns(requestedCols, index, table))
                .collect(Collectors.toList());

        // get indices with hash keys that match a predicate
        candidateIndices.stream()
                .filter(index -> hasPredicateForColumn.test(index.getHashKey()) && hasValidHashKeyValues.test(index.getHashKey()))
                .forEach(hashKeyMatchesBuilder::add);
        List<DynamoDBIndex> hashKeyMatches = hashKeyMatchesBuilder.build();

        // collect range key matches
        List<DynamoDBIndex> rangeKeyMatches = Stream.concat(
                tableIndex.getRangeKey().filter(hasPredicateForColumn).stream().map(key -> tableIndex),
                candidateIndices.stream().filter(index -> index.getRangeKey().filter(hasPredicateForColumn).isPresent())
        ).collect(Collectors.toList());

        // return first index where both hash and range key can be specified with predicates
        for (DynamoDBIndex index : hashKeyMatches) {
            if (rangeKeyMatches.contains(index)) {
                return index;
            }
        }
        // else return the first index with a hash key predicate, or the original table if there are none
        return hashKeyMatches.isEmpty() ? tableIndex : hashKeyMatches.get(0);
    }

    /*
    Adds a value to the value accumulator.
     */
    private static void bindValue(String columnName, Object value, List<AttributeValue> accumulator, DDBRecordMetadata recordMetadata)
    {
        accumulator.add(DDBTypeUtils.toAttributeValue(DDBTypeUtils.convertArrowTypeIfNecessary(columnName, value, recordMetadata)));
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

    private static void validateColumnRange(Range range)
    {
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    break;
                case EXACTLY:
                    break;
                case BELOW:
                    throw new AthenaConnectorException("Low marker should never use BELOW bound", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
                default:
                    throw new AssertionError("Unhandled lower bound: " + range.getLow().getBound());
            }
        }
        if (!range.getHigh().isUpperUnbounded()) {
            switch (range.getHigh().getBound()) {
                case ABOVE:
                    throw new AthenaConnectorException("High marker should never use ABOVE bound", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
                case EXACTLY:
                    break;
                case BELOW:
                    break;
                default:
                    throw new AssertionError("Unhandled upper bound: " + range.getHigh().getBound());
            }
        }
    }

    /**
     * Generates a filter expression for a single column given a {@link ValueSet} predicate for that column.
     *
     * @param originalColumnName the column name
     * @param predicate the associated predicate
     * @param accumulator the value accumulator to add values to
     * @param valueNameProducer the value name producer to generate value aliases with
     * @param recordMetadata object containing any necessary metadata from the glue table
     * @param columnIsSortKey whether or not the originalColumnName column is a sort key
     * @return the generated filter expression
     */
    public static String generateSingleColumnFilter(String originalColumnName, ValueSet predicate, List<AttributeValue> accumulator,
            IncrementingValueNameProducer valueNameProducer, DDBRecordMetadata recordMetadata, boolean columnIsSortKey)
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
                    continue;
                }
                validateColumnRange(range);
                List<String> rangeConjuncts = new ArrayList<>();
                // DDB Only supports one condition per sort key, so here we attempt to combine the situations wherever we
                // have both inclusive upper and lower bounds (BETWEEN).
                if (range.getLow().getBound().equals(Marker.Bound.EXACTLY) && range.getHigh().getBound().equals(Marker.Bound.EXACTLY)) {
                    String startBetweenPredicate = toPredicate(originalColumnName, "BETWEEN", range.getLow().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata);
                    String endBetweenPredicate = valueNameProducer.getNext();
                    bindValue(originalColumnName, range.getHigh().getValue(), accumulator, recordMetadata);
                    rangeConjuncts.add(startBetweenPredicate);
                    rangeConjuncts.add(endBetweenPredicate);
                }
                else {
                    // Otherwise in this situation, since we still want to
                    // push down, we will just push down one of the bounds
                    // here if it is a sort key.
                    // We will prioritize upper bounds because they should
                    // theoretically result in fewer matches vs lower bounds.
                    boolean upperBoundConditionAdded = false;
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(originalColumnName, "<=", range.getHigh().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                upperBoundConditionAdded = true;
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(originalColumnName, "<", range.getHigh().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                upperBoundConditionAdded = true;
                                break;
                        }
                    }
                    // We can always add the lower bound if the column is not
                    // a sort key.
                    // But if it is a sort key, then we can only add it if we
                    // have not already added the upper bound.
                    boolean canAddLowerBound = (!columnIsSortKey || !upperBoundConditionAdded);
                    if (canAddLowerBound && !range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(originalColumnName, ">", range.getLow().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(originalColumnName, ">=", range.getLow().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata));
                                break;
                        }
                    }
                }
                // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add("(" + AND_JOINER.join(rangeConjuncts) + ")");
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
            String values = COMMA_JOINER.join(Stream.generate(valueNameProducer::getNext).limit(singleValues.size()).collect(Collectors.toList()));
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
     * Generates a filter expression for a single column from Substrait predicates.
     */
    public static String generateSingleColumnFilter(String originalColumnName, List<ColumnPredicate> predicates,
                                                    List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                                    DDBRecordMetadata recordMetadata, boolean columnIsSortKey)
    {
        if (predicates.isEmpty()) {
            return null;
        }

        String columnName = aliasColumn(originalColumnName);

        // Handle null checks for single predicates
        if (predicates.size() == 1) {
            String nullCheckFilter = handleNullCheckPredicates(originalColumnName, columnName, predicates.get(0),
                    accumulator, valueNameProducer, recordMetadata);
            if (nullCheckFilter != null) {
                return nullCheckFilter;
            }
        }

        List<String> disjuncts = new ArrayList<>();

        if (areEquatablePredicates(predicates)) {
            String equalityFilter = buildEqualityFilter(originalColumnName, columnName, predicates,
                    accumulator, valueNameProducer, recordMetadata);
            if (equalityFilter != null) {
                disjuncts.add(equalityFilter);
            }
        }
        else {
            String rangeFilter = buildRangeFilter(originalColumnName, predicates, accumulator,
                    valueNameProducer, recordMetadata, columnIsSortKey);
            if (rangeFilter != null) {
                disjuncts.add(rangeFilter);
            }
        }

        return formatDisjuncts(disjuncts);
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
                String columnFilter = generateSingleColumnFilter(columnName, predicate.getValue(), accumulator, valueNameProducer, recordMetadata, false);
                builder.add(columnFilter);
            }
        }
        ImmutableList<String> filters = builder.build();
        if (!filters.isEmpty()) {
            return AND_JOINER.join(filters);
        }
        return null;
    }

    public static String generateFilterExpressionForPlan(Set<String> columnsToIgnore, Map<String, List<ColumnPredicate>> predicates, List<AttributeValue> accumulator,
                                                  IncrementingValueNameProducer valueNameProducer, DDBRecordMetadata recordMetadata)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map.Entry<String, List<ColumnPredicate>> predicate : predicates.entrySet()) {
            String columnName = predicate.getKey();
            if (!columnsToIgnore.contains(columnName)) {
                String columnFilter = generateSingleColumnFilter(columnName, predicate.getValue(), accumulator, valueNameProducer, recordMetadata, false);
                if (columnFilter != null) {
                    builder.add(columnFilter);
                }
            }
        }
        ImmutableList<String> filters = builder.build();
        if (!filters.isEmpty()) {
            return AND_JOINER.join(filters);
        }
        return null;
    }

    public static boolean indexContainsAllRequiredColumns(List<String> requestedCols, DynamoDBIndex index, DynamoDBTable table)
    {
        if (index == null || table == null || requestedCols == null) {
            return false;
        }

        // ALL projection type includes all columns
        if (index.getProjectionType() == ProjectionType.ALL) {
            return true;
        }

        Set<String> indexColumns = new HashSet<>();

        // Add table keys (always projected)
        indexColumns.add(table.getHashKey());
        table.getRangeKey().ifPresent(indexColumns::add);
        
        // Add index keys
        indexColumns.add(index.getHashKey());
        index.getRangeKey().ifPresent(indexColumns::add);
        
        // Add projection-specific columns
        if (index.getProjectionType() == ProjectionType.INCLUDE) {
            indexColumns.addAll(index.getProjectionAttributeNames());
        }

        return indexColumns.containsAll(requestedCols);
    }

    private static boolean areEquatablePredicates(List<ColumnPredicate> predicates)
    {
        for (ColumnPredicate predicate : predicates) {
            SubstraitOperator substraitOperator = predicate.getOperator();
            if (!(substraitOperator == SubstraitOperator.EQUAL  || substraitOperator == SubstraitOperator.NOT_EQUAL)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAllowListPredicate(List<ColumnPredicate> predicates)
    {
        for (ColumnPredicate predicate : predicates) {
            if (predicate.getOperator() == SubstraitOperator.NOT_EQUAL) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasStrictIncludeRange(List<ColumnPredicate> predicates)
    {
        boolean hasLower = false;
        boolean hasUpper = false;

        for (ColumnPredicate predicate : predicates) {
            SubstraitOperator op = predicate.getOperator();
            if (op == SubstraitOperator.GREATER_THAN_OR_EQUAL_TO) {
                hasLower = true;
            }
            else if (op == SubstraitOperator.LESS_THAN_OR_EQUAL_TO) {
                hasUpper = true;
            }
        }

        return hasLower && hasUpper;
    }

    private static Pair<ColumnPredicate, ColumnPredicate> getIncludedRangeBounds(List<ColumnPredicate> predicates)
    {
        ColumnPredicate lowerBound = null;
        ColumnPredicate upperBound = null;

        for (ColumnPredicate predicate : predicates) {
            SubstraitOperator op = predicate.getOperator();
            if (op == SubstraitOperator.GREATER_THAN_OR_EQUAL_TO) {
                lowerBound = predicate;
            }
            else if (op == SubstraitOperator.LESS_THAN_OR_EQUAL_TO) {
                upperBound = predicate;
            }
        }

        return Pair.of(lowerBound, upperBound);
    }

    private static ColumnPredicate getColumnPredicate(List<ColumnPredicate> columnPredicates, Predicate<ColumnPredicate> hasMatchedCriteria)
    {
        for (ColumnPredicate predicate : columnPredicates) {
            if (hasMatchedCriteria.test(predicate)) {
                return predicate;
            }
        }
        return null;
    }
    
    /**
     * Handles null check predicates (IS_NULL, IS_NOT_NULL).
     */
    private static String handleNullCheckPredicates(String originalColumnName, String columnName, ColumnPredicate predicate,
                                                   List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                                   DDBRecordMetadata recordMetadata)
    {
        SubstraitOperator substraitOperator = predicate.getOperator();
        if (substraitOperator == SubstraitOperator.IS_NOT_NULL) {
            return "(attribute_exists(" + columnName + ") AND " + 
                   toPredicate(originalColumnName, "=", null, accumulator, valueNameProducer.getNext(), recordMetadata) + ")";
        }
        if (substraitOperator == SubstraitOperator.IS_NULL) {
            return "(attribute_not_exists(" + columnName + ") OR " + 
                   toPredicate(originalColumnName, "<>", null, accumulator, valueNameProducer.getNext(), recordMetadata) + ")";
        }
        return null;
    }
    
    /**
     * Builds equality filter expressions (=, !=, IN, NOT IN).
     */
    private static String buildEqualityFilter(String originalColumnName, String columnName, List<ColumnPredicate> predicates,
                                            List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                            DDBRecordMetadata recordMetadata)
    {
        boolean isAllowlist = isAllowListPredicate(predicates);
        List<Object> singleValues = predicates.stream()
                .map(ColumnPredicate::getValue)
                .collect(Collectors.toList());
        
        if (singleValues.size() == 1) {
            return toPredicate(originalColumnName, isAllowlist ? "=" : "<>", 
                             getOnlyElement(singleValues), accumulator, valueNameProducer.getNext(), recordMetadata);
        }
        else if (singleValues.size() > 1) {
            for (Object value : singleValues) {
                bindValue(originalColumnName, value, accumulator, recordMetadata);
            }
            String values = COMMA_JOINER.join(Stream.generate(valueNameProducer::getNext)
                                            .limit(singleValues.size()).collect(Collectors.toList()));
            return (isAllowlist ? "" : "NOT ") + columnName + " IN (" + values + ")";
        }
        return null;
    }
    
    /**
     * Builds range filter expressions (>, >=, <, <=, BETWEEN).
     */
    private static String buildRangeFilter(String originalColumnName, List<ColumnPredicate> predicates,
                                         List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                         DDBRecordMetadata recordMetadata, boolean columnIsSortKey)
    {
        List<String> rangeConjuncts = new ArrayList<>();
        
        if (hasStrictIncludeRange(predicates)) {
            Pair<ColumnPredicate, ColumnPredicate> rangePredicates = getIncludedRangeBounds(predicates);
            String startBetweenPredicate = toPredicate(originalColumnName, "BETWEEN",
                    rangePredicates.getLeft().getValue(), accumulator, valueNameProducer.getNext(), recordMetadata);
            String endBetweenPredicate = valueNameProducer.getNext();
            bindValue(originalColumnName, rangePredicates.getRight().getValue(), accumulator, recordMetadata);
            rangeConjuncts.add(startBetweenPredicate);
            rangeConjuncts.add(endBetweenPredicate);
        }
        else {
            addUpperBoundCondition(originalColumnName, predicates, accumulator, valueNameProducer, recordMetadata, rangeConjuncts);
            boolean upperBoundAdded = !rangeConjuncts.isEmpty();
            addLowerBoundCondition(originalColumnName, predicates, accumulator, valueNameProducer, recordMetadata, 
                                 rangeConjuncts, columnIsSortKey, upperBoundAdded);
        }
        
        checkState(!rangeConjuncts.isEmpty());
        return "(" + AND_JOINER.join(rangeConjuncts) + ")";
    }
    
    /**
     * Formats disjuncts into final filter expression.
     */
    private static String formatDisjuncts(List<String> disjuncts)
    {
        if (disjuncts.isEmpty()) {
            return null;
        }
        // DDB doesn't like redundant parentheses
        if (disjuncts.size() == 1) {
            return disjuncts.get(0);
        }
        return "(" + OR_JOINER.join(disjuncts) + ")";
    }
    
    /**
     * Adds upper bound conditions to range conjuncts.
     */
    private static void addUpperBoundCondition(String originalColumnName, List<ColumnPredicate> predicates,
                                             List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                             DDBRecordMetadata recordMetadata, List<String> rangeConjuncts)
    {
        Predicate<ColumnPredicate> isLessThanPredicate = predicate ->
                (predicate.getOperator() == SubstraitOperator.LESS_THAN || predicate.getOperator() == SubstraitOperator.LESS_THAN_OR_EQUAL_TO);
        ColumnPredicate upperBoundPredicate = getColumnPredicate(predicates, isLessThanPredicate);
        
        if (upperBoundPredicate != null) {
            switch (upperBoundPredicate.getOperator()) {
                case LESS_THAN_OR_EQUAL_TO:
                    rangeConjuncts.add(toPredicate(originalColumnName, "<=", upperBoundPredicate.getValue(), 
                                                 accumulator, valueNameProducer.getNext(), recordMetadata));
                    break;
                case LESS_THAN:
                    rangeConjuncts.add(toPredicate(originalColumnName, "<", upperBoundPredicate.getValue(), 
                                                 accumulator, valueNameProducer.getNext(), recordMetadata));
                    break;
            }
        }
    }
    
    /**
     * Adds lower bound conditions to range conjuncts.
     */
    private static void addLowerBoundCondition(String originalColumnName, List<ColumnPredicate> predicates,
                                             List<AttributeValue> accumulator, IncrementingValueNameProducer valueNameProducer,
                                             DDBRecordMetadata recordMetadata, List<String> rangeConjuncts,
                                             boolean columnIsSortKey, boolean upperBoundAdded)
    {
        // We can always add the lower bound if the column is not a sort key.
        // But if it is a sort key, then we can only add it if we have not already added the upper bound.
        boolean canAddLowerBound = (!columnIsSortKey || !upperBoundAdded);
        
        if (!canAddLowerBound) {
            return;
        }
        
        Predicate<ColumnPredicate> isGreaterThan = predicate ->
                (predicate.getOperator() == SubstraitOperator.GREATER_THAN_OR_EQUAL_TO || predicate.getOperator() == SubstraitOperator.GREATER_THAN);
        ColumnPredicate lowerBoundPredicate = getColumnPredicate(predicates, isGreaterThan);
        
        if (lowerBoundPredicate != null) {
            switch (lowerBoundPredicate.getOperator()) {
                case GREATER_THAN:
                    rangeConjuncts.add(toPredicate(originalColumnName, ">", lowerBoundPredicate.getValue(), 
                                                 accumulator, valueNameProducer.getNext(), recordMetadata));
                    break;
                case GREATER_THAN_OR_EQUAL_TO:
                    rangeConjuncts.add(toPredicate(originalColumnName, ">=", lowerBoundPredicate.getValue(), 
                                                 accumulator, valueNameProducer.getNext(), recordMetadata));
                    break;
            }
        }
    }
}
