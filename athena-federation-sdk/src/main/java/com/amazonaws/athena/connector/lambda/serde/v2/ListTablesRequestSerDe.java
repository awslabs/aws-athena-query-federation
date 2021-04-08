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

import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class ListTablesRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMA_NAME_FIELD = "schemaName";
    private static final String NEXT_TOKEN_FIELD = "nextToken";

    private ListTablesRequestSerDe(){}

    static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;

        Serializer(FederatedIdentitySerDe.Serializer identitySerializer)
        {
            super(ListTablesRequest.class, identitySerializer);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
        }

        @Override
        protected void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ListTablesRequest listTablesRequest = (ListTablesRequest) federationRequest;

            jgen.writeStringField(SCHEMA_NAME_FIELD, listTablesRequest.getSchemaName());
            // Since nextToken is optional, it should always be serialized last.
            writeNextTokenField(listTablesRequest.getNextToken(), jgen);
        }

        /**
         * Serializes the value of nextToken if present in the request.
         * @param nextToken The starting point (table name) for the next paginated response.
         * @param jgen The JSON generator used to write the value of nextToken.
         * @throws IOException An error was encountered writing the value of nextToken.
         */
        private void writeNextTokenField(Optional<String> nextToken, JsonGenerator jgen)
                throws IOException
        {
            if (nextToken.isPresent()) {
                jgen.writeStringField(NEXT_TOKEN_FIELD, nextToken.get());
            }
        }
    }

    static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;

        Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer)
        {
            super(ListTablesRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            String schemaName = getNextStringField(jparser, SCHEMA_NAME_FIELD);
            // Since nextToken is optional, it should always be deserialized last.
            String nextToken = getNextTokenField(jparser);

            return new ListTablesRequest(identity, queryId, catalogName, schemaName, nextToken);
        }

        /**
         * Deserializes the value of nextToken if present in the input stream.
         * @param jparser The JSON parser used to parse the nextToken from the input stream.
         * @return The String value of the nextToken if present in the input stream, or null if it's not.
         * @throws IOException An error was encountered reading the value of nextToken.
         */
        private String getNextTokenField(JsonParser jparser)
                throws IOException
        {
            if (JsonToken.END_OBJECT.equals(jparser.nextToken()) ||
                    !jparser.getCurrentName().equals(NEXT_TOKEN_FIELD)) {
                // nextToken is not present in the input stream.
                return null;
            }

            jparser.nextToken();

            return jparser.getValueAsString();
        }
    }
}
