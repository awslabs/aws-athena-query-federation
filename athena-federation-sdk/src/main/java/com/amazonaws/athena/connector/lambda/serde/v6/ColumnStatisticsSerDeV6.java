/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.serde.v6;

import com.amazonaws.athena.connector.lambda.data.ColumnStatistic;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Optional;

public class ColumnStatisticsSerDeV6
{
    private static final String MIN_FIELD = "min";
    private static final String MAX_FIELD = "max";
    private static final String COLUMN_SIZE_FIELD = "columnSize";
    private static final String NULL_PROBABILITY_FIELD = "nullProbability";

    public static final class Serializer extends BaseSerializer<ColumnStatistic> implements VersionedSerDe.Serializer<ColumnStatistic>
    {
        public Serializer()
        {
            super(ColumnStatistic.class);
        }

        @Override
        public void doSerialize(ColumnStatistic columnStatistic, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            writeOptionalDouble(jgen, columnStatistic.getMin(), MIN_FIELD);
            writeOptionalDouble(jgen, columnStatistic.getMax(), MAX_FIELD);
            writeOptionalDouble(jgen, columnStatistic.getColumnSize(), COLUMN_SIZE_FIELD);
            writeOptionalDouble(jgen, columnStatistic.getNullProbability(), NULL_PROBABILITY_FIELD);
        }

        private void writeOptionalDouble(JsonGenerator jgen, Optional<Double> field, String fieldName) throws IOException
        {
            if (field.isPresent()) {
                jgen.writeNumberField(fieldName, field.get());
            }
            else {
                jgen.writeNullField(fieldName);
            }
        }
    }

    public static final class Deserializer extends BaseDeserializer<ColumnStatistic> implements VersionedSerDe.Deserializer<ColumnStatistic>
    {
        public Deserializer()
        {
            super(ColumnStatistic.class);
        }

        @Override
        public ColumnStatistic doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            Optional<Double> min = getNextDoubleOptionalField(jparser, MIN_FIELD);
            Optional<Double> max = getNextDoubleOptionalField(jparser, MAX_FIELD);
            Optional<Double> columnSize = getNextDoubleOptionalField(jparser, COLUMN_SIZE_FIELD);
            Optional<Double> nullProbability = getNextDoubleOptionalField(jparser, NULL_PROBABILITY_FIELD);

            return new ColumnStatistic(min, max, nullProbability, columnSize);
        }
    }
}
