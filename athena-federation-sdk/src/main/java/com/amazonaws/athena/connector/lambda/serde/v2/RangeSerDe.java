/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v2;

import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class RangeSerDe
{
    private static final String LOW_FIELD = "low";
    private static final String HIGH_FIELD = "high";

    private RangeSerDe() {}

    public static final class Serializer extends BaseSerializer<Range>
    {
        private final MarkerSerDe.Serializer markerSerializer;

        public Serializer(MarkerSerDe.Serializer markerSerializer)
        {
            super(Range.class);
            this.markerSerializer = requireNonNull(markerSerializer, "markerSerializer is null");
        }

        @Override
        public void doSerialize(Range range, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeFieldName(LOW_FIELD);
            markerSerializer.serialize(range.getLow(), jgen, provider);

            jgen.writeFieldName(HIGH_FIELD);
            markerSerializer.serialize(range.getHigh(), jgen, provider);
        }
    }

    public static final class Deserializer extends BaseDeserializer<Range>
    {
        private final MarkerSerDe.Deserializer markerDeserializer;

        public Deserializer(MarkerSerDe.Deserializer markerDeserializer)
        {
            super(Range.class);
            this.markerDeserializer = requireNonNull(markerDeserializer, "markerDeserializer is null");
        }

        @Override
        public Range doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, LOW_FIELD);
            Marker low = markerDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, HIGH_FIELD);
            Marker high = markerDeserializer.deserialize(jparser, ctxt);

            return new Range(low, high);
        }
    }
}
