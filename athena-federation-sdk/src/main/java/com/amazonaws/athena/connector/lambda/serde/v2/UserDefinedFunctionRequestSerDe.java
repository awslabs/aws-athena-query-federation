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
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest;
import com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionType;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class UserDefinedFunctionRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String INPUT_RECORDS_FIELD = "inputRecords";
    private static final String OUTPUT_SCHEMA_FIELD = "outputSchema";
    private static final String METHOD_NAME_FIELD = "methodName";
    private static final String FUNCTION_TYPE_FIELD = "functionType";

    private UserDefinedFunctionRequestSerDe() {}

    public static final class Serializer extends TypedSerializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final VersionedSerDe.Serializer<Block> blockSerializer;
        private final VersionedSerDe.Serializer<Schema> schemaSerializer;

        public Serializer(
                FederatedIdentitySerDe.Serializer identitySerializer,
                VersionedSerDe.Serializer<Block> blockSerializer,
                VersionedSerDe.Serializer<Schema> schemaSerializer)
        {
            super(FederationRequest.class, UserDefinedFunctionRequest.class);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerializer is null");
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            UserDefinedFunctionRequest userDefinedFunctionRequest = (UserDefinedFunctionRequest) federationRequest;

            jgen.writeFieldName(IDENTITY_FIELD);
            identitySerializer.serialize(federationRequest.getIdentity(), jgen, provider);

            jgen.writeFieldName(INPUT_RECORDS_FIELD);
            blockSerializer.serialize(userDefinedFunctionRequest.getInputRecords(), jgen, provider);

            jgen.writeFieldName(OUTPUT_SCHEMA_FIELD);
            schemaSerializer.serialize(userDefinedFunctionRequest.getOutputSchema(), jgen, provider);

            jgen.writeStringField(METHOD_NAME_FIELD, userDefinedFunctionRequest.getMethodName());
            jgen.writeStringField(FUNCTION_TYPE_FIELD, userDefinedFunctionRequest.getFunctionType().toString());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final VersionedSerDe.Deserializer<Block> blockDeserializer;
        private final VersionedSerDe.Deserializer<Schema> schemaDeserializer;

        public Deserializer(
                FederatedIdentitySerDe.Deserializer identityDeserializer,
                VersionedSerDe.Deserializer<Block> blockDeserializer,
                VersionedSerDe.Deserializer<Schema> schemaDeserializer)
        {
            super(FederationRequest.class, UserDefinedFunctionRequest.class);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockDeserializer is null");
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
        }

        @Override
        protected FederationRequest doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, IDENTITY_FIELD);
            FederatedIdentity identity = identityDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, INPUT_RECORDS_FIELD);
            Block inputRecords = blockDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, OUTPUT_SCHEMA_FIELD);
            Schema outputSchema = schemaDeserializer.deserialize(jparser, ctxt);

            String methodName = getNextStringField(jparser, METHOD_NAME_FIELD);
            UserDefinedFunctionType functionType = UserDefinedFunctionType.valueOf(getNextStringField(jparser, FUNCTION_TYPE_FIELD));

            return new UserDefinedFunctionRequest(identity, inputRecords, outputSchema, methodName, functionType);
        }
    }
}
