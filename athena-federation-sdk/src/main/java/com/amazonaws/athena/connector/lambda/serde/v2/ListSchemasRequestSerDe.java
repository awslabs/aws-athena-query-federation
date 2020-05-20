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

import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class ListSchemasRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";

    private ListSchemasRequestSerDe(){}

    static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;

        Serializer(FederatedIdentitySerDe.Serializer identitySerializer)
        {
            super(ListSchemasRequest.class, identitySerializer);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
        }

        @Override
        protected void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            // no other fields
        }
    }

    static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;

        Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer)
        {
            super(ListSchemasRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            return new ListSchemasRequest(identity, queryId, catalogName);
        }
    }
}
