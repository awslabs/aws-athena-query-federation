package com.amazonaws.athena.connector.lambda.domain.predicate;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;

/**
 * Used to apply predicates to values inside your connector. Ideally you would also be able to push
 * constraints into your source system (e.g. RDBMS via SQL). For each value you'd like to write for a given row,
 * you call the 'apply' function on this class and if the values for all columns in the row return 'true' that
 * indicates that the row passes the constraints.
 * <p>
 * After being used, ConstraintEvaluator instance must be closed to ensure no Apache Arrow resources used by
 * Markers that it creates as part of evaluation are leaked.
 *
 * @note This abstraction works well for the associative predicates that are made available to your connector
 * today but will likely require enhancement as we expose more sophisticated predicates (e.g. col1 + col2 < 100)
 * in the future. Additionally, we do not support constraints on complex types are this time.
 * <p>
 * For usage examples, please see the ExampleRecordHandler or connectors like athena-redis.
 * <p>
 * TODO: We can improve the filtering performance of ConstraintEvaluator by refactoring how ValueSets and Markers works.
 * @see ValueSet for details on how Constraints are represented and individually applied.
 */
public class ConstraintEvaluator
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ConstraintEvaluator.class);

    private final Constraints constraints;

    //Used to reduce the object overhead of constraints by sharing blocks across Markers.
    //This is a byproduct of the way we are using Apache Arrow to hold Markers which are essentially
    //a single value blocks. This factory allows us to represent a Marker (aka a single value) as
    //a row in a shared block to improve memory and perf.
    private final MarkerFactory markerFactory;
    //Holds the type for each field.
    private final Map<String, ArrowType> typeMap = new HashMap<>();

    public ConstraintEvaluator(BlockAllocator allocator, Schema schema, Constraints constraints)
    {
        this.constraints = constraints;
        for (Field next : schema.getFields()) {
            typeMap.put(next.getName(), next.getType());
        }
        markerFactory = new MarkerFactory(allocator);
    }

    /**
     * This convenience method builds an empty Evaluator that can be useful when no constraints are present.
     *
     * @return An empty ConstraintEvaluator which always returns true when applied to a value for any field.
     */
    public static ConstraintEvaluator emptyEvaluator()
    {
        return new ConstraintEvaluator(null, SchemaBuilder.newBuilder().build(), new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT));
    }

    /**
     * Used check if the provided value passes all constraints on the given field.
     *
     * @param fieldName The name of the field whoe's constraints we'd like to apply to the value.
     * @param value The value to test.
     * @return True if the value passed all constraints for the given field, False otherwise. This method also returns
     * True if the field has no constraints, including if the field is unknown.
     */
    public boolean apply(String fieldName, Object value)
    {
        try {
            ValueSet constraint = constraints.getSummary().get(fieldName);
            if (constraint != null && typeMap.get(fieldName) != null) {
                try (Marker marker = markerFactory.createNullable(typeMap.get(fieldName),
                        value,
                        Marker.Bound.EXACTLY)) {
                    return constraint.containsValue(marker);
                }
            }

            return true;
        }
        catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
    }

    public Optional<ConstraintProjector> makeConstraintProjector(String fieldName)
    {
        ValueSet constraint = constraints.getSummary().get(fieldName);
        if (constraint != null && typeMap.get(fieldName) != null) {
            return Optional.of((Object value) -> constraint.containsValue(value));
        }
        return Optional.empty();
    }

    /**
     * Frees any Apache Arrow resources held by this Constraint Evaluator.
     *
     * @throws Exception
     */
    @Override
    public void close()
            throws Exception
    {
        markerFactory.close();
    }
}
