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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.amazonaws.athena.connector.lambda.serde.BaseDeserializer;
import com.amazonaws.athena.connector.lambda.serde.BaseSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public final class FunctionNameSerDeV4
{
    private static final String FUNCTION_NAME_FIELD = "functionName";

    private FunctionNameSerDeV4() {}

    public static final class Serializer extends BaseSerializer<FunctionName> implements VersionedSerDe.Serializer<FunctionName>
    {
        public Serializer()
        {
            super(FunctionName.class);
        }

        @Override
        public void doSerialize(FunctionName functionName, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            jgen.writeStringField(FUNCTION_NAME_FIELD, functionName.getFunctionName());
        }
    }

    public static final class Deserializer extends BaseDeserializer<FunctionName> implements VersionedSerDe.Deserializer<FunctionName>
    {
        public Deserializer()
        {
            super(FunctionName.class);
        }

        @Override
        public FunctionName doDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String functionName = getNextStringField(jparser, FUNCTION_NAME_FIELD);
            return new FunctionName(functionName);
        }
    }
}
