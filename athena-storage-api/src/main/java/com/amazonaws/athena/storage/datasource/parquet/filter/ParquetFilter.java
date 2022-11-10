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
package com.amazonaws.athena.storage.datasource.parquet.filter;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.storage.common.FilterExpression;
import com.amazonaws.athena.storage.common.StorageObjectField;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound.EXACTLY;

/**
 * Usually a non-JDBC connector we use MetadataHandler and RecordHandler, which are not specific to
 * JDBC. Thus, we have to handle constraint within the connector. When we fetch constraint summery,
 * we only have constraints with an AND operator. Other operators are handled by Athena itself
 * <p>
 * This is because AND, and OR operators are not still working with parquet
 */
@SuppressWarnings("unused")
public class ParquetFilter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFilter.class);

    /**
     * Expression evaluator for this filter to aid filter push down
     */
    private final ParquetConstraintEvaluator evaluator;

    /**
     * List of {@link Field} instances
     */
    private final List<Field> fields;

    /**
     * A column-column index map to store an index for the associated column name
     */
    private final Map<String, Integer> columnIndices = new HashMap<>();

    private final List<FilterExpression> and = new ArrayList<>();

    private final List<FilterExpression> or = new ArrayList<>();

    /**
     * Construct an instance of this type with schema fields, fields from the instance of {@link MessageType}, and a {@link Split}.
     *
     * @param schema      An instance of {@link Schema} to retrieve list of Fields
     * @param messageType The real schema of the underlying PARQUET file (to be used to retrieve column information)
     * @param split       An instance of {@link Split} that holds underlying PARQUET file name
     */
    public ParquetFilter(Schema schema, MessageType messageType, Split split)
    {
        this.fields = schema.getFields().stream()
                .filter(c -> !split.getProperties().containsKey(c.getName()))
                .collect(Collectors.toList());

        List<ColumnDescriptor> columns = messageType.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getPath()[0];
            columnIndices.put(columnName, i);
        }
        this.evaluator = new ParquetConstraintEvaluator(and);
    }

    /**
     * Construct an instance of this type with schema fields, fields from the instance of {@link MessageType}, and a {@link Split}.
     *
     * @param schema      An instance of {@link Schema} to retrieve list of Fields
     * @param objectSchema The shorter version of real schema of the underlying PARQUET file (to be used to retrieve column information)
     * @param partitionFieldValueMap A map of partition field(s) and value(s)
     */
    public ParquetFilter(Schema schema, StorageObjectSchema objectSchema, Map<String, String> partitionFieldValueMap)
    {
        this.fields = schema.getFields().stream()
                .filter(c -> !partitionFieldValueMap.containsKey(c.getName()))
                .collect(Collectors.toList());
        List<StorageObjectField> schemaFields = objectSchema.getFields();
        for (StorageObjectField field : schemaFields) {
            columnIndices.put(field.getColumnName(), field.getColumnIndex());
        }
        this.evaluator = new ParquetConstraintEvaluator(and);
    }

    /**
     * Creates the required expressions based on provided {@link Constraints}, which is basically the part of SQL query running
     * within Athena (where clauses)
     *
     * @param tableInfo   An instance of {@link TableName} that contains schema and table names
     * @param split       An instance of {@link Split} that holds underlying PARQUET file name
     * @param constraints An instance of {@link Constraints} that summarizes the where clauses from the SQL query (if any)
     * @return An instance of {@link ParquetConstraintEvaluator}
     */
    public ConstraintEvaluator evaluator(TableName tableInfo, Split split, Constraints constraints)
    {
        return evaluator(tableInfo, split.getProperties(), constraints);
    }

    public ConstraintEvaluator evaluator(TableName tableInfo, Map<String, String> partitionFieldValueMap,
                                         Constraints constraints)
    {
        LOGGER.info("Filter::ParquetFilter|Constraint summary:\n{}", constraints.getSummary());
        List<FilterExpression> expressions = toConjuncts(tableInfo,
                constraints, partitionFieldValueMap);
        LOGGER.info("Filter::ParquetFilter|Generated expressions:\n{}", expressions);
        if (!expressions.isEmpty()) {
            expressions.forEach(this::addToAnd);
        }
        return new ParquetConstraintEvaluatorWrapper(evaluator);
    }

    /**
     * @return List of {@link Field}
     */
    public List<Field> fields()
    {
        return fields == null ? List.of() : fields;
    }

    // helpers

    /**
     * Prepares the expressions based on the provided arguments.
     *
     * @param tableName      An instance of {@link TableName}
     * @param constraints    An instance of {@link Constraints} that is a summary of where clauses (if any)
     * @param partitionFieldValueMap A key-value that holds partition column if any
     * @return A list of {@link ParquetExpression}
     */
    private List<FilterExpression> toConjuncts(TableName tableName,
                                                Constraints constraints,
                                                Map<String, String> partitionFieldValueMap)
    {
        List<FilterExpression> conjuncts = new ArrayList<>();
        for (Field column : fields) {
            if (partitionFieldValueMap.containsKey(column.getName())) {
                continue;
            }
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    conjuncts.addAll(addParquetExpressions(column.getName(), valueSet, column.getType()));
                }
            }
        }
        return conjuncts;
    }

    /**
     * Add one or more {@link ParquetExpression} based on {@link ValueSet}
     *
     * @param columnName The name of the column
     * @param valueSet   An instance of {@link ValueSet}
     * @param type       Type of the column
     * @return List of {@link ParquetExpression}
     */
    private List<FilterExpression> addParquetExpressions(String columnName, ValueSet valueSet, ArrowType type)
    {
        List<FilterExpression> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return List.of(new IsNullExpression(columnIndices.get(columnName), columnName));
            }

            if (!valueSet.isNullAllowed() && isHighLowEquals(valueSet)
                    && valueSet.getRanges().getSpan().getLow().getBound().equals(EXACTLY)) {
                return List.of(new EqualsExpression(columnIndices.get(columnName), columnName, valueSet.getRanges().getSpan().getLow().getValue()));
            }

            if (!valueSet.isNullAllowed()) {
                disjuncts.add(new IsNotNullExpression(columnIndices.get(columnName), columnName));
            }

            Range rangeSpan = ((SortedRangeSet) valueSet).getSpan();
            if (!valueSet.isNullAllowed() && rangeSpan.getLow().isLowerUnbounded() && rangeSpan.getHigh().isUpperUnbounded()) {
                return List.of(new IsNotNullExpression(columnIndices.get(columnName), columnName));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                disjuncts.add(new GreaterThanExpression(columnIndices.get(columnName), columnName, (Comparable<?>) range.getLow().getValue()));
                                break;
                            case EXACTLY:
                                disjuncts.add(new GreaterThanOrEqualExpression(columnIndices.get(columnName), columnName, (Comparable<?>) range.getLow().getValue()));
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
                                LOGGER.debug("Util=ParquetFilterUtil|Method=toPredicate|Message=Returning ltEq FilterPredicate");
                                disjuncts.add(new LessThanOrEqualExpression(columnIndices.get(columnName), columnName, (Comparable<?>) range.getHigh().getValue()));
                                break;
                            case BELOW:
                                LOGGER.debug("Util=ParquetFilterUtil|Method=toPredicate|Message=Returning lt FilterPredicate");
                                disjuncts.add(new LessThanExpression(columnIndices.get(columnName), columnName, (Comparable<?>) range.getHigh().getValue()));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                }
            }
            if (singleValues.size() == 1) {
                disjuncts.add(new EqualsExpression(columnIndices.get(columnName), columnName, singleValues.get(0)));
            }
            else if (singleValues.size() > 1) {
                List<Object> values = new ArrayList<>(singleValues);
                disjuncts.add(new AnyExpression(columnIndices.get(columnName), columnName, values));
            }
        }
        disjuncts.forEach(this::addToOr);
        return or;
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

    private void addToAnd(FilterExpression expression)
    {
        if (!and.contains(expression)) {
            and.add(expression);
        }
    }

    protected final void addToOr(FilterExpression expression)
    {
        or.add(expression);
    }
}
