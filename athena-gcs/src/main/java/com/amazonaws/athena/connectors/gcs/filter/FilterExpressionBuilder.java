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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
     * @return A map of column names to its value constraint set.
     */
    public static Map<String, Optional<Set<String>>> getConstraintsForPartitionedColumns(List<Column> partitionColumns, Constraints constraints)
    {
        LOGGER.info("Constraint summaries: \n{}", constraints.getSummary());
        return partitionColumns.stream().collect(Collectors.toMap(
            column -> column.getName(),
            column -> singleValuesStringSetFromValueSet(constraints.getSummary().get(column.getName())),
            // Also we are forced to use Optional here because Collectors.toMap() doesn't allow null values to
            // be passed into the merge function (it asserts that the values are not null)
            // We shouldn't have duplicates but just merge the sets if we do.
            (value1, value2) -> {
                if (!value1.isPresent() && !value2.isPresent()) {
                    return Optional.empty();
                }
                Set<String> value1Set = value1.orElse(java.util.Collections.emptySet());
                Set<String> value2Set = value2.orElse(java.util.Collections.emptySet());
                return Optional.of(com.google.common.collect.ImmutableSet.<String>builder().addAll(value1Set).addAll(value2Set).build());
            },
            () -> new java.util.TreeMap<String, Optional<Set<String>>>(String.CASE_INSENSITIVE_ORDER)
        ));
    }

    /**
     * Returns a Set of Strings from the valueSet
     *
     * @param valueSet   An instance of {@link ValueSet}
     * @return Set<String> from the valueSet
     */
    private static Optional<Set<String>> singleValuesStringSetFromValueSet(ValueSet valueSetIn)
    {
        return Optional.ofNullable(valueSetIn).map(valueSet -> valueSet.getRanges().getOrderedRanges().stream()
          .filter(range -> range.isSingleValue())
          .map(range -> range.getLow().getValue())
          .map(value -> (org.apache.arrow.vector.util.Text) value) // purposefully cast to Text to cause an exception if its not Text
          .map(value -> value.toString())
          .collect(Collectors.toSet()));
    }

    private FilterExpressionBuilder() {}
}
