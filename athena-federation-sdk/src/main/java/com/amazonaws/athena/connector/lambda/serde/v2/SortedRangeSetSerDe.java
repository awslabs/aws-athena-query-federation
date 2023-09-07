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

import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class SortedRangeSetSerDe
{
    private static final String TYPE_FIELD = "type";
    private static final String RANGES_FIELD = "ranges";
    private static final String NULL_ALLOWED_FIELD = "nullAllowed";

    private SortedRangeSetSerDe() {}

    public static final class Serializer extends TypedSerializer<ValueSet>
    {
        private final ArrowTypeSerDe.Serializer arrowTypeSerializer;
        private final RangeSerDe.Serializer rangeSerializer;

        public Serializer(ArrowTypeSerDe.Serializer arrowTypeSerializer, RangeSerDe.Serializer rangeSerializer)
        {
            super(ValueSet.class, SortedRangeSet.class);
            this.arrowTypeSerializer = requireNonNull(arrowTypeSerializer, "arrowTypeSerializer is null");
            this.rangeSerializer = requireNonNull(rangeSerializer, "rangeSerializer is null");
        }

        @Override
        protected void doTypedSerialize(ValueSet valueSet, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            SortedRangeSet sortedRangeSet = (SortedRangeSet) valueSet;

            jgen.writeFieldName(TYPE_FIELD);
            arrowTypeSerializer.serialize(sortedRangeSet.getType(), jgen, provider);

            jgen.writeFieldName(RANGES_FIELD);
            jgen.writeStartArray();
            for (Range range : sortedRangeSet.getOrderedRanges()) {
                rangeSerializer.serialize(range, jgen, provider);
            }
            jgen.writeEndArray();

            jgen.writeBooleanField(NULL_ALLOWED_FIELD, sortedRangeSet.isNullAllowed());
        }
    }

    public static final class Deserializer extends TypedDeserializer<ValueSet>
    {
        private final ArrowTypeSerDe.Deserializer arrowTypeDeserializer;
        private final RangeSerDe.Deserializer rangeDeserializer;

        public Deserializer(ArrowTypeSerDe.Deserializer arrowTypeDeserializer, RangeSerDe.Deserializer rangeDeserializer)
        {
            super(ValueSet.class, SortedRangeSet.class);
            this.arrowTypeDeserializer = requireNonNull(arrowTypeDeserializer, "arrowTypeDeserializer is null");
            this.rangeDeserializer = requireNonNull(rangeDeserializer, "rangeDeserializer is null");
        }

        @Override
        protected ValueSet doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, TYPE_FIELD);
            ArrowType type = arrowTypeDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, RANGES_FIELD);
            validateArrayStart(jparser);
            ImmutableList.Builder<Range> rangesList = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                rangesList.add(rangeDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            boolean nullAllowed = getNextBoolField(jparser, NULL_ALLOWED_FIELD);

            return SortedRangeSet.copyOf(type, rangesList.build(), nullAllowed);
        }
    }
}
