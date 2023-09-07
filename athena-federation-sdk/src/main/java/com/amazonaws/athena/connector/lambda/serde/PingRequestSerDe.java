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
package com.amazonaws.athena.connector.lambda.serde;

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * This SerDe must remain backwards and forwards compatible in order as this call
 * is first and the SerDe version has not been set yet.
 */
public final class PingRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String QUERY_ID_FIELD = "queryId";
    // new fields should only be appended to the end for backwards compatibility

    private PingRequestSerDe() {}

    public static class Serializer extends TypedSerializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;

        public Serializer(FederatedIdentitySerDe.Serializer identitySerializer)
        {
            super(FederationRequest.class, PingRequest.class);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            PingRequest pingRequest = (PingRequest) federationRequest;

            jgen.writeFieldName(IDENTITY_FIELD);
            identitySerializer.serialize(pingRequest.getIdentity(), jgen, provider);

            jgen.writeStringField(CATALOG_NAME_FIELD, pingRequest.getCatalogName());
            jgen.writeStringField(QUERY_ID_FIELD, pingRequest.getQueryId());
            // new fields should only be appended to the end for backwards compatibility
        }
    }

    public static class Deserializer extends TypedDeserializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;

        public Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer)
        {
            super(FederationRequest.class, PingRequest.class);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
        }

        @Override
        public FederationRequest deserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            validateObjectStart(jparser.getCurrentToken());
            return doDeserialize(jparser, ctxt);
            // do not validate object end to allow forwards compatibility
        }

        @Override
        protected FederationRequest doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, IDENTITY_FIELD);
            FederatedIdentity identity = identityDeserializer.deserialize(jparser, ctxt);

            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
            String queryId = getNextStringField(jparser, QUERY_ID_FIELD);

            return new PingRequest(identity, catalogName, queryId);
        }
    }
}
