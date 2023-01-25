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
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.services.glue.model.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is used to build Filter Expression to handle the constraints on partition folder
 *
 */
public class FilterExpressionBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterExpressionBuilder.class);

    /**
     * Prepares the expressions based on the provided arguments.
     *
     * @param partitionColumns       partition columns from the Glue table
     * @param constraints            An instance of {@link Constraints} that is a summary of where clauses (if any)
     * @return A list of {@link EqualsExpression}
     */
    public static List<AbstractExpression> getExpressions(List<Column> partitionColumns, Constraints constraints)
    {
        LOGGER.info("Constraint summaries: \n{}", constraints.getSummary());
        List<AbstractExpression> conjuncts = new ArrayList<>();
        for (Column column : partitionColumns) {
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                LOGGER.info("Value set for column {} was {}", column, valueSet);
                if (valueSet != null) {
                    conjuncts.add(getFilterExpressions(column.getName(), valueSet));
                }
            }
        }
        return conjuncts;
    }

    /**
     * Returns an {@link AbstractExpression} based on {@link ValueSet}
     *
     * @param columnName The name of the column
     * @param valueSet   An instance of {@link ValueSet}
     * @return {@link AbstractExpression}
     */
    private static AbstractExpression getFilterExpressions(String columnName, ValueSet valueSet)
    {
        LOGGER.info("FilterExpressionBuilder::getFilterExpressions -> Evaluating and adding expression for col {} with valueSet {}", columnName, valueSet);
        LOGGER.info("FilterExpressionBuilder::getFilterExpressions -> Bound {}", valueSet.getRanges().getSpan().getLow().getBound());

        List<Object> singleValues = valueSet.getRanges().getOrderedRanges().stream()
          .filter(range -> range.isSingleValue())
          .map(range -> range.getLow().getValue())
          .collect(Collectors.toList());

        if (singleValues.size() == 1) {
            return new EqualsExpression(columnName.toLowerCase(), singleValues.get(0));
        }

        return new AnyExpression(columnName.toLowerCase(), singleValues.stream().map(Object::toString).collect(Collectors.toList()));
    }

    private FilterExpressionBuilder() {}
}
