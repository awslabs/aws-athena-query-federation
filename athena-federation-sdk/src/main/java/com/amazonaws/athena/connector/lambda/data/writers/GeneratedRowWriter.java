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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.BigIntFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.BitFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.DateDayFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.DateMilliFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.DecimalFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.Float4FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.Float8FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.IntFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.SmallIntFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.TinyIntFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.VarBinaryFieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.VarCharFieldWriter;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;

public class GeneratedRowWriter
{
    private static final Logger logger = LoggerFactory.getLogger(GeneratedRowWriter.class);
    private final LinkedHashMap<String, Extractor> extractors = new LinkedHashMap<>();
    private final LinkedHashMap<String, FieldWriterFactory> fieldWriterFactories = new LinkedHashMap<>();
    private List<FieldWriter> fieldWriters = new ArrayList<>();
    private LinkedHashMap<String, ConstraintProjector> constraints = new LinkedHashMap<>();

    //holds the last block that was used to generate our FieldWriters
    private Block block;

    private GeneratedRowWriter(RowWriterBuilder builder)
    {
        this.extractors.putAll(builder.extractors);
        this.fieldWriterFactories.putAll(builder.fieldWriterFactories);
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

    public static RowWriterBuilder newBuilder()
    {
        return new RowWriterBuilder(new Constraints(ImmutableMap.of(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT));
    }

    public boolean writeRow(Block block, int rowNum, Object context)
            throws Exception
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
        FieldWriterFactory factory = fieldWriterFactories.get(fieldName);

        if (factory != null) {
            return factory.create(vector, extractor, constraint);
        }

        if (extractor == null) {
            throw new IllegalStateException("Missing extractor for field[" + fieldName + "]");
        }

        switch (fieldType) {
            case INT:
                return new IntFieldWriter((IntExtractor) extractor, (IntVector) vector, constraint);
            case BIGINT:
                return new BigIntFieldWriter((BigIntExtractor) extractor, (BigIntVector) vector, constraint);
            case DATEMILLI:
                return new DateMilliFieldWriter((DateMilliExtractor) extractor, (DateMilliVector) vector, constraint);
            case DATEDAY:
                return new DateDayFieldWriter((DateDayExtractor) extractor, (DateDayVector) vector, constraint);
            case TINYINT:
                return new TinyIntFieldWriter((TinyIntExtractor) extractor, (TinyIntVector) vector, constraint);
            case SMALLINT:
                return new SmallIntFieldWriter((SmallIntExtractor) extractor, (SmallIntVector) vector, constraint);
            case FLOAT4:
                return new Float4FieldWriter((Float4Extractor) extractor, (Float4Vector) vector, constraint);
            case FLOAT8:
                return new Float8FieldWriter((Float8Extractor) extractor, (Float8Vector) vector, constraint);
            case DECIMAL:
                return new DecimalFieldWriter((DecimalExtractor) extractor, (DecimalVector) vector, constraint);
            case BIT:
                return new BitFieldWriter((BitExtractor) extractor, (BitVector) vector, constraint);
            case VARCHAR:
                return new VarCharFieldWriter((VarCharExtractor) extractor, (VarCharVector) vector, constraint);
            case VARBINARY:
                return new VarBinaryFieldWriter((VarBinaryExtractor) extractor, (VarBinaryVector) vector, constraint);
            default:
                throw new RuntimeException(fieldType + " is not supported");
        }
    }

    public static class RowWriterBuilder
    {
        private final Constraints constraints;
        //some consumers may care about ordering
        private final LinkedHashMap<String, Extractor> extractors = new LinkedHashMap<>();
        //some consumers may care about ordering
        private final LinkedHashMap<String, FieldWriterFactory> fieldWriterFactories = new LinkedHashMap<>();

        private RowWriterBuilder(Constraints constraints)
        {
            this.constraints = constraints;
        }

        public RowWriterBuilder withExtractor(String fieldName, Extractor extractor)
        {
            extractors.put(fieldName, extractor);
            return this;
        }

        /**
         * Used to override the default FieldWriter for the given field. For example, you might use this if your source
         * stores DateDay using java type Y but our default FieldWriter for DateDay only accept type Z even though Apache
         * Arrow has a native setter for type Y. By proving your own FieldWriterFactory you can avoid the extra step
         * of translating to an intermediate type.
         *
         * @param fieldName The name of the field who's factory you'd like to override.
         * @param factory The factory you'd like to use instead of the default.
         * @return This builder.
         */
        public RowWriterBuilder withFieldWriterFactory(String fieldName, FieldWriterFactory factory)
        {
            fieldWriterFactories.put(fieldName, factory);
            return this;
        }

        public GeneratedRowWriter build()
        {
            return new GeneratedRowWriter(this);
        }
    }
}
