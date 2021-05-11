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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
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

public final class ConstraintsSerDe
{
    private static final String SUMMARY_FIELD = "summary";

    private ConstraintsSerDe(){}

    public static final class Serializer extends BaseSerializer<Constraints>
    {
        private final ValueSetSerDe.Serializer valueSetSerializer;

        public Serializer(ValueSetSerDe.Serializer valueSetSerializer)
        {
            super(Constraints.class);
            this.valueSetSerializer = requireNonNull(valueSetSerializer, "valueSetSerDe is null");
        }

        @Override
        public void doSerialize(Constraints constraints, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeObjectFieldStart(SUMMARY_FIELD);
            for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
                jgen.writeFieldName(entry.getKey());
                valueSetSerializer.serialize(entry.getValue(), jgen, provider);
            }
            jgen.writeEndObject();
        }
    }

    public static final class Deserializer extends BaseDeserializer<Constraints>
    {
        private final ValueSetSerDe.Deserializer valueSetDeserializer;

        public Deserializer(ValueSetSerDe.Deserializer valueSetDeserializer)
        {
            super(Constraints.class);
            this.valueSetDeserializer = requireNonNull(valueSetDeserializer, "valueSetSerDe is null");
        }

        @Override
        public Constraints doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, SUMMARY_FIELD);
            validateObjectStart(jparser.nextToken());
            ImmutableMap.Builder<String, ValueSet> summaryMap = ImmutableMap.builder();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String column = jparser.getCurrentName();
                summaryMap.put(column, valueSetDeserializer.deserialize(jparser, ctxt));
            }

            return new Constraints(summaryMap.build());
        }
    }
}
