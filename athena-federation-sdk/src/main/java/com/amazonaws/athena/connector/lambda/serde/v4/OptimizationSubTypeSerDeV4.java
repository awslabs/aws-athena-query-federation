/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.List;

public class OptimizationSubTypeSerDeV4
{
    private static final String SUBTYPE_FIELD = "subType";
    private static final String PROPERTIES_FIELD = "properties";

    private OptimizationSubTypeSerDeV4() {}

    public static final class Serializer extends BaseSerializer<OptimizationSubType> implements VersionedSerDe.Serializer<OptimizationSubType>
    {
        public Serializer()
        {
            super(OptimizationSubType.class);
        }

        @Override
        public void doSerialize(OptimizationSubType optimizationSubType, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(SUBTYPE_FIELD, optimizationSubType.getSubType());
            writeStringArray(jgen, PROPERTIES_FIELD, optimizationSubType.getProperties());
        }
    }

    public static final class Deserializer extends BaseDeserializer<OptimizationSubType> implements VersionedSerDe.Deserializer<OptimizationSubType>
    {
        public Deserializer()
        {
            super(OptimizationSubType.class);
        }

        @Override
        public OptimizationSubType doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String optimization = getNextStringField(jparser, SUBTYPE_FIELD);
            List<String> properties = getNextStringArray(jparser, PROPERTIES_FIELD);

            return new OptimizationSubType(optimization, properties);
        }
    }
}
