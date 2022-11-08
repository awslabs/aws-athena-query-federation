/*-
 * #%L
 * athena-hive
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.storage.datasource.csv;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound.EXACTLY;

/**
 * Currently it only supports returning only a single predicate
 * <p>
 * This is because AND, and OR operators are not still working with parquet
 */
@SuppressWarnings("unused")
public class CsvFilter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvFilter.class);
    private final CsvConstraintEvaluator evaluator = new CsvConstraintEvaluator();
    private List<String> fields;

    public CsvFilter()
    {
    }

    /**
     * Builds the constraint evaluator for the query made against a CSV file (as a table in Athena). It builds the
     * evaluator based on the constraint build from the where clauses by the Athena echo system.
     *
     * @param recordsRequest An instance of {@link ReadRecordsRequest}
     * @return An instance of {@link CsvConstraintEvaluator}. If the built evaluator is empty it returns an instance of {@link NoopCsvEvaluator}
     * that does nothing but spills directory
     */
    public ConstraintEvaluator evaluator(ReadRecordsRequest recordsRequest)
    {
        final Constraints constraints = recordsRequest.getConstraints();
        LOGGER.info("Util::evaluator|Constraint summary:\n{}", constraints.getSummary());
        final Schema tableSchema = recordsRequest.getSchema();
        final Split split = recordsRequest.getSplit();

        this.fields = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .collect(Collectors.toList());
        LOGGER.debug("Util::evaluator|Selected fields are \n{}", fields);

        List<FilterExpression> expressions = toConjuncts(recordsRequest.getTableName(), tableSchema.getFields(),
                constraints, split.getProperties());
        if (!expressions.isEmpty()) {
            LOGGER.debug("CsvFilter::evaluator|Adding all determined expressions to CsvConstraintEvaluator");
            expressions.forEach(evaluator::addToAnd);
            evaluator.or().clear();
        }
        if (this.evaluator.isEmptyEvaluator()) {
            LOGGER.debug("Returning empty evaluator as nothing to evaluate");
            return new NoopCsvEvaluator();
        }
        return new CsvConstraintEvaluatorProxy(this.evaluator);
    }

    /**
     * Builds the constraint evaluator for the query made against a CSV file (as a table in Athena). It builds the
     * evaluator based on the provided arguments
     *
     * @param tableSchema An instance of {@link Schema}
     * @param constraints An instance of {@link Constraints}
     * @param tableName   An instance of {@link TableName} that contains both schema and table names
     * @param split       A {@link Split} with row offset and count
     * @return An instance of {@link CsvConstraintEvaluator}. If the built evaluator is empty it returns an instance of {@link NoopCsvEvaluator}
     * that does nothing but spills directory
     */
    public ConstraintEvaluator evaluator(Schema tableSchema, Constraints constraints, TableName tableName,
                                            Split split)
    {
        return evaluator(tableSchema, constraints, tableName, split.getProperties());
    }

    /**
     * Builds the constraint evaluator for the query made against a CSV file (as a table in Athena). It builds the
     * evaluator based on the provided arguments
     *
     * @param tableSchema An instance of {@link Schema}
     * @param constraints An instance of {@link Constraints}
     * @param tableName   An instance of {@link TableName} that contains both schema and table names
     * @param partitionFieldValueMap A map of partition field name(s) and value(s) if any.
     * @return An instance of {@link CsvConstraintEvaluator}. If the built evaluator is empty it returns an instance of {@link NoopCsvEvaluator}
     * that does nothing but spills directory
     */
    public ConstraintEvaluator evaluator(Schema tableSchema, Constraints constraints, TableName tableName,
                                         Map<String, String> partitionFieldValueMap)
    {
        LOGGER.info("Util::evaluator|Constraint summary:{}\n", constraints.getSummary());
        this.fields = tableSchema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !partitionFieldValueMap.containsKey(c))
                .collect(Collectors.toList());
        LOGGER.debug("Util::evaluator|Selected fields are {}\n", fields);

        List<FilterExpression> expressions = toConjuncts(tableName, tableSchema.getFields(),
                constraints, partitionFieldValueMap);
        if (!expressions.isEmpty()) {
            LOGGER.debug("CsvFilter::evaluator|Adding all determined expressions to CsvConstraintEvaluator");
            expressions.forEach(evaluator::addToAnd);
            evaluator.or().clear();
        }
        if (this.evaluator.isEmptyEvaluator()) {
            LOGGER.debug("Returning empty evaluator as nothing to evaluate");
            return new NoopCsvEvaluator();
        }
        return new CsvConstraintEvaluatorProxy(this.evaluator);
    }

    /**
     * @return A list of field names
     */
    public List<String> fields()
    {
        return fields == null ? List.of() : fields;
    }

    // helpers
    private List<FilterExpression> toConjuncts(TableName tableName,
                                               List<Field> columns, Constraints constraints,
                                               Map<String, String> partitionSplit)
    {
        List<FilterExpression> conjuncts = new ArrayList<>();
        for (Field column : columns) {
            if (partitionSplit.containsKey(column.getName())) {
                continue;
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.addAll(addCsvExpressions(column.getName(), valueSet));
                }
            }
        }
        return conjuncts;
    }

    /**
     * Builds and list CSV expression base on column name and values being passed in the constraint (where clause)
     *
     * @param columnName Name of the column
     * @param valueSet   An instance of {@link ValueSet}
     * @return A list of {@link FilterExpression} if any built, or an empty list
     */
    private List<FilterExpression> addCsvExpressions(String columnName, ValueSet valueSet)
    {
        List<FilterExpression> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                LOGGER.debug("CsvFilter::addPredicateExpression|Adding IsNullExpression when valueSet.isNone()");
                return List.of(new IsNullExpression(columnName));
            }

            if (!valueSet.isNullAllowed() && isHighLowEquals(valueSet)
                    && valueSet.getRanges().getSpan().getLow().getBound().equals(EXACTLY)) {
                return List.of(new EqualsExpression(columnName, valueSet.getRanges().getSpan().getLow().getValue().toString()));
            }

            if (!valueSet.isNullAllowed()) {
                LOGGER.debug("CsvFilter::addPredicateExpression|Adding IsNotNullExpression when valueSet.isNullAllowed()");
                disjuncts.add(new IsNotNullExpression(columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                LOGGER.debug("CsvFilter::addPredicateExpression|Adding IsNotNullExpression");
                return List.of(new IsNotNullExpression(columnName));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
            }
            // Add-back all the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(new EqualsExpression(columnName, Iterables.getOnlyElement(singleValues).toString()));
            }
            else if (singleValues.size() > 1) {
                List<String> values = new ArrayList<>();
                for (Object value : singleValues) {
                    values.add(value.toString());
                }
                disjuncts.add(new AnyExpression(columnName, values));
            }
        }
        disjuncts.forEach(evaluator::addToOr);
        return evaluator.or();
    }

    /**
     * To determine whether an EQUAL expression need to be built
     *
     * @param valueSet An instance of {@link ValueSet}
     * @return True if both the high and low value are equal
     */
    private boolean isHighLowEquals(ValueSet valueSet)
    {
        Range range = valueSet.getRanges().getSpan();
        return (range.getLow() != null && range.getHigh() != null && range.getLow().equals(range.getHigh()));
    }
}
