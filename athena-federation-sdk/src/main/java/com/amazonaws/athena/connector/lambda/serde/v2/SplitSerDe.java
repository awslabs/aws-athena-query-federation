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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

final class SplitSerDe
{
    private static final String SPILL_LOCATION_FIELD = "spillLocation";
    private static final String ENCRYPTION_KEY_FIELD = "encryptionKey";
    private static final String PROPERTIES_FIELD = "properties";

    private SplitSerDe(){}

    static final class Serializer extends BaseSerializer<Split>
    {
        private final SpillLocationSerDe.Serializer spillLocationSerializer;
        private final EncryptionKeySerDe.Serializer encryptionKeySerializer;

        Serializer(SpillLocationSerDe.Serializer spillLocationSerializer, EncryptionKeySerDe.Serializer encryptionKeySerializer)
        {
            super(Split.class);
            this.spillLocationSerializer = requireNonNull(spillLocationSerializer, "spillLocationSerializer is null");
            this.encryptionKeySerializer = requireNonNull(encryptionKeySerializer, "encryptionKeySerializer is null");
        }

        @Override
        protected void doSerialize(Split split, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeFieldName(SPILL_LOCATION_FIELD);
            spillLocationSerializer.serialize(split.getSpillLocation(), jgen, provider);

            jgen.writeFieldName(ENCRYPTION_KEY_FIELD);
            if (split.getEncryptionKey() != null) {
                encryptionKeySerializer.serialize(split.getEncryptionKey(), jgen, provider);
            }
            else {
                jgen.writeNull();
            }

            jgen.writeObjectFieldStart(PROPERTIES_FIELD);
            for (Map.Entry<String, String> entry : split.getProperties().entrySet()) {
                jgen.writeFieldName(entry.getKey());
                jgen.writeString(entry.getValue());
            }
            jgen.writeEndObject();
        }
    }

    static final class Deserializer extends BaseDeserializer<Split>
    {
        private final SpillLocationSerDe.Deserializer spillLocationDeserializer;
        private final EncryptionKeySerDe.Deserializer encryptionKeyDeserializer;

        Deserializer(SpillLocationSerDe.Deserializer spillLocationDeserializer, EncryptionKeySerDe.Deserializer encryptionKeyDeserializer)
        {
            super(Split.class);
            this.spillLocationDeserializer = requireNonNull(spillLocationDeserializer, "spillLocationDeserializer is null");
            this.encryptionKeyDeserializer = requireNonNull(encryptionKeyDeserializer, "encryptionKeyDeserializer is null");
        }

        @Override
        protected Split doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, SPILL_LOCATION_FIELD);
            SpillLocation spillLocation = spillLocationDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, ENCRYPTION_KEY_FIELD);
            EncryptionKey encryptionKey = encryptionKeyDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, PROPERTIES_FIELD);
            validateObjectStart(jparser.nextToken());
            ImmutableMap.Builder<String, String> propertiesMap = ImmutableMap.builder();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String key = jparser.getValueAsString();
                jparser.nextToken();
                String value = jparser.getValueAsString();
                propertiesMap.put(key, value);
            }

            return new Split(spillLocation, encryptionKey, propertiesMap.build());
        }
    }
}
