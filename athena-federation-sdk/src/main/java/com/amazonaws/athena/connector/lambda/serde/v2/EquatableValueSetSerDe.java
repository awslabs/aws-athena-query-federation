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
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class EquatableValueSetSerDe
{
    private static final String VALUE_BLOCK_FIELD = "valueBlock";
    private static final String WHITELIST_FIELD = "whiteList";
    private static final String NULL_ALLOWED_FIELD = "nullAllowed";

    private EquatableValueSetSerDe() {}

    public static final class Serializer extends TypedSerializer<ValueSet>
    {
        private final VersionedSerDe.Serializer<Block> blockSerializer;

        public Serializer(VersionedSerDe.Serializer<Block> blockSerializer)
        {
            super(ValueSet.class, EquatableValueSet.class);
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerDe is null");
        }

        @Override
        protected void doTypedSerialize(ValueSet valueSet, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;

            jgen.writeFieldName(VALUE_BLOCK_FIELD);
            blockSerializer.serialize(equatableValueSet.getValueBlock(), jgen, provider);

            jgen.writeBooleanField(WHITELIST_FIELD, equatableValueSet.isWhiteList());
            jgen.writeBooleanField(NULL_ALLOWED_FIELD, equatableValueSet.nullAllowed);
        }
    }

    public static final class Deserializer extends TypedDeserializer<ValueSet>
    {
        private final VersionedSerDe.Deserializer<Block> blockDeserializer;

        public Deserializer(VersionedSerDe.Deserializer<Block> blockDeserializer)
        {
            super(ValueSet.class, EquatableValueSet.class);
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockSerDe is null");
        }

        @Override
        protected ValueSet doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, VALUE_BLOCK_FIELD);
            Block valueBlock = blockDeserializer.deserialize(jparser, ctxt);

            boolean whiteList = getNextBoolField(jparser, WHITELIST_FIELD);
            boolean nullAllowed = getNextBoolField(jparser, NULL_ALLOWED_FIELD);

            return new EquatableValueSet(valueBlock, whiteList, nullAllowed);
        }
    }
}
