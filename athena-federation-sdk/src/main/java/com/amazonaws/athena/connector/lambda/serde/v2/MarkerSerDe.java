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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class MarkerSerDe
{
    private static final String VALUE_BLOCK_FIELD = "valueBlock";
    private static final String BOUND_FIELD = "bound";
    private static final String NULL_VALUE_FIELD = "nullValue";

    private MarkerSerDe() {}

    public static final class Serializer extends BaseSerializer<Marker>
    {
        private final VersionedSerDe.Serializer<Block> blockSerializer;

        public Serializer(VersionedSerDe.Serializer<Block> blockSerializer)
        {
            super(Marker.class);
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerializer is null");
        }

        @Override
        public void doSerialize(Marker marker, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeFieldName(VALUE_BLOCK_FIELD);
            blockSerializer.serialize(marker.getValueBlock(), jgen, provider);

            jgen.writeStringField(BOUND_FIELD, marker.getBound().toString());
            jgen.writeBooleanField(NULL_VALUE_FIELD, marker.isNullValue());
        }
    }

    public static final class Deserializer extends BaseDeserializer<Marker>
    {
        private final VersionedSerDe.Deserializer<Block> blockDeserializer;

        public Deserializer(VersionedSerDe.Deserializer<Block> blockDeserializer)
        {
            super(Marker.class);
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockDeserializer is null");
        }

        @Override
        public Marker doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, VALUE_BLOCK_FIELD);
            Block valueBlock = blockDeserializer.deserialize(jparser, ctxt);

            Marker.Bound bound = Marker.Bound.valueOf(getNextStringField(jparser, BOUND_FIELD));
            boolean nullValue = getNextBoolField(jparser, NULL_VALUE_FIELD);

            return new Marker(valueBlock, bound, nullValue);
        }
    }
}
