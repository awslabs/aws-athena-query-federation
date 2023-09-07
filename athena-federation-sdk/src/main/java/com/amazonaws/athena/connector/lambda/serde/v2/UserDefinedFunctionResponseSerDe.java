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
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class UserDefinedFunctionResponseSerDe
{
    private static final String RECORDS_FIELD = "records";
    private static final String METHOD_NAME_FIELD = "methodName";

    private UserDefinedFunctionResponseSerDe() {}

    public static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final VersionedSerDe.Serializer<Block> blockSerializer;

        public Serializer(VersionedSerDe.Serializer<Block> blockSerializer)
        {
            super(FederationResponse.class, UserDefinedFunctionResponse.class);
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            UserDefinedFunctionResponse userDefinedFunctionResponse = (UserDefinedFunctionResponse) federationResponse;

            jgen.writeFieldName(RECORDS_FIELD);
            blockSerializer.serialize(userDefinedFunctionResponse.getRecords(), jgen, provider);

            jgen.writeStringField(METHOD_NAME_FIELD, userDefinedFunctionResponse.getMethodName());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final VersionedSerDe.Deserializer<Block> blockDeserializer;

        public Deserializer(VersionedSerDe.Deserializer<Block> blockDeserializer)
        {
            super(FederationResponse.class, UserDefinedFunctionResponse.class);
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, RECORDS_FIELD);
            Block records = blockDeserializer.deserialize(jparser, ctxt);

            String methodName = getNextStringField(jparser, METHOD_NAME_FIELD);

            return new UserDefinedFunctionResponse(records, methodName);
        }
    }
}
