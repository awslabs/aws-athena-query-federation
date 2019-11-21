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
package com.amazonaws.athena.connector.lambda.data.writers;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.BigIntFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.IntFieldWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GeneratedRowWriter
{
    private static final Logger logger = LoggerFactory.getLogger(GeneratedRowWriter.class);
    private final LinkedHashMap<String, Extractor> extractors = new LinkedHashMap<>();
    private List<FieldWriter> fieldWriters = new ArrayList<>();
    private LinkedHashMap<String, ConstraintProjector> constraints = new LinkedHashMap<>();

    //holds the last block that was used to generate our FieldWriters
    private Block block;

    private GeneratedRowWriter(RowWriterBuilder builder)
    {
        this.extractors.putAll(builder.extractors);
        if (builder.constraints != null && builder.constraints.getSummary() != null) {
            for (Map.Entry<String, ValueSet> next : builder.constraints.getSummary().entrySet()) {
                constraints.put(next.getKey(), makeConstraintProjector(next.getValue()));
            }
        }
    }

    public static RowWriterBuilder newBuilder(Constraints constraints)
    {
        return new RowWriterBuilder(constraints);
    }

    public boolean writeRow(Block block, int rowNum, Object context)
    {
        checkAndRecompile(block);

        boolean matched = true;
        for (FieldWriter next : fieldWriters) {
            matched &= next.write(context, rowNum);
        }
        return matched;
    }

    private ConstraintProjector makeConstraintProjector(ValueSet constraint)
    {
        return (Object value) -> constraint.containsValue(value);
    }

    private void checkAndRecompile(Block block)
    {
        if (this.block != block) {
            logger.info("recompile: Detected a new block, rebuilding field writers so they point to the correct Arrow vectors.");
            this.block = block;
            fieldWriters.clear();
            for (FieldVector vector : block.getFieldVectors()) {
                fieldWriters.add(makeFieldWriter(vector));
            }
        }
    }

    private FieldWriter makeFieldWriter(FieldVector vector)
    {
        Field field = vector.getField();
        String fieldName = field.getName();
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        Extractor extractor = extractors.get(fieldName);
        ConstraintProjector constraint = constraints.get(fieldName);

        switch (fieldType) {
            case INT:
                return new IntFieldWriter((IntExtractor) extractor, (IntVector) vector, constraint);
            case BIGINT:
                return new BigIntFieldWriter((BigIntExtractor) extractor, (BigIntVector) vector, constraint);
            default:
                throw new RuntimeException(fieldType + " is not supported");
        }
    }

    public static class RowWriterBuilder
    {
        private final Constraints constraints;
        //some consumers may can about ordering
        private final LinkedHashMap<String, Extractor> extractors = new LinkedHashMap<>();

        private RowWriterBuilder(Constraints constraints)
        {
            this.constraints = constraints;
        }

        public RowWriterBuilder withExtractor(String fieldName, Extractor extractor)
        {
            extractors.put(fieldName, extractor);
            return this;
        }
    }
}
