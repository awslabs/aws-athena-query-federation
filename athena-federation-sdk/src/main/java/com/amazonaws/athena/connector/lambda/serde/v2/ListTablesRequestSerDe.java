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

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static java.util.Objects.requireNonNull;

public final class ListTablesRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMA_NAME_FIELD = "schemaName";
    private static final String PAGE_SIZE_FIELD = "pageSize";
    private static final String NEXT_TOKEN_FIELD = "nextToken";

    private ListTablesRequestSerDe(){}

    public static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;

        public Serializer(FederatedIdentitySerDe.Serializer identitySerializer)
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
            jgen.writeStringField(NEXT_TOKEN_FIELD, listTablesRequest.getNextToken());
            jgen.writeNumberField(PAGE_SIZE_FIELD, listTablesRequest.getPageSize());
        }
    }

    public static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;

        public Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer)
        {
            super(ListTablesRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            String schemaName = getNextStringField(jparser, SCHEMA_NAME_FIELD);

            /**
             * TODO: This logic must be modified in V3 of the SDK to enforce the presence of nextToken and pageSize in
             *       the JSON contract.
             *       For backwards compatibility with V2 of the SDK, we will first verify that the JSON contract
             *       contains the nextToken and pageSize arguments, and if not, set the default values for them.
             */
            String nextToken = null;
            int pageSize = UNLIMITED_PAGE_SIZE_VALUE;
            if (!JsonToken.END_OBJECT.equals(jparser.nextToken()) &&
                    jparser.getCurrentName().equals(NEXT_TOKEN_FIELD)) {
                jparser.nextToken();
                nextToken = jparser.getValueAsString();
                pageSize = getNextIntField(jparser, PAGE_SIZE_FIELD);
            }

            return new ListTablesRequest(identity, queryId, catalogName, schemaName, nextToken, pageSize);
        }
    }
}
