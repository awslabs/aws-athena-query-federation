/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs.filter;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound.EXACTLY;

/**
 * Usually a non-JDBC connector we use MetadataHandler and RecordHandler, which are not specific to
 * JDBC. Thus, we have to handle constraint within the connector. When we fetch constraint summery,
 * we only have constraints with an AND operator. Other operators are handled by Athena itself
 * <p>
 * This is because AND, and OR operators are not still working with parquet
 */
@SuppressWarnings("unused")
public class FilterExpressionBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterExpressionBuilder.class);

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

    public FilterExpressionBuilder(Schema schema)
    {
        this.fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            String columnName = fields.get(i).getName();
            columnIndices.put(columnName, i);
        }
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
     * @param constraints            An instance of {@link Constraints} that is a summary of where clauses (if any)
     * @param partitionFieldValueMap A key-value that holds partition column if any
     * @return A list of {@link FilterExpression}
     */
    public List<FilterExpression> getExpressions(Constraints constraints,
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
                LOGGER.debug("Value set for column {} was {}", column, valueSet);
                if (valueSet != null) {
                    conjuncts.addAll(addParquetExpressions(column.getName(), valueSet, column.getType()));
                }
            }
        }
        return conjuncts;
    }

    /**
     * Add one or more {@link FilterExpression} based on {@link ValueSet}
     *
     * @param columnName The name of the column
     * @param valueSet   An instance of {@link ValueSet}
     * @param type       Type of the column
     * @return List of {@link FilterExpression}
     */
    private List<FilterExpression> addParquetExpressions(String columnName, ValueSet valueSet, ArrowType type)
    {
        List<FilterExpression> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        if (valueSet instanceof SortedRangeSet) {
            if (!valueSet.isNullAllowed() && isHighLowEquals(valueSet)
                    && valueSet.getRanges().getSpan().getLow().getBound().equals(EXACTLY)) {
                return List.of(new EqualsExpression(columnIndices.get(columnName), columnName, valueSet.getRanges().getSpan().getLow().getValue()));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
            }
            if (singleValues.size() == 1) {
                disjuncts.add(new EqualsExpression(columnIndices.get(columnName), columnName, singleValues.get(0)));
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
