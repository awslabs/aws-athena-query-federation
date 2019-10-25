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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ConstraintEvaluator
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(ConstraintEvaluator.class);

    private final Constraints constraints;

    //Used to reduce the object overhead of constraints by sharing blocks across Markers.
    private final MarkerFactory markerFactory;
    private final Map<String, ArrowType> typeMap = new HashMap<>();

    public ConstraintEvaluator(BlockAllocator allocator, Schema schema, Constraints constraints)
    {
        this.constraints = constraints;
        for (Field next : schema.getFields()) {
            typeMap.put(next.getName(), next.getType());
        }
        markerFactory = new MarkerFactory(allocator);
    }

    public boolean apply(String fieldName, Object value)
    {
        try {
            ValueSet constraint = constraints.getSummary().get(fieldName);
            if (constraint != null && typeMap.get(fieldName) != null) {
                try (Marker marker = markerFactory.create(typeMap.get(fieldName),
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

    @Override
    public void close()
            throws Exception
    {
        markerFactory.close();
    }
}
