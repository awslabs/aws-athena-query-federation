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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * This SerDe must remain backwards and forwards compatible in order as this call
 * is first and the SerDe version has not been set yet.
 */
public final class PingResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String SOURCE_TYPE_FIELD = "sourceType";
    private static final String CAPABILITIES_FIELD = "capabilities";
    private static final String SERDE_VERSION_FIELD = "serDeVersion";
    // new fields should only be appended to the end for forwards compatibility

    private PingResponseSerDe(){}

    public static final class Serializer extends TypedSerializer<FederationResponse>
    {
        public Serializer()
        {
            super(FederationResponse.class, PingResponse.class);
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            PingResponse pingResponse = (PingResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, pingResponse.getCatalogName());
            jgen.writeStringField(QUERY_ID_FIELD, pingResponse.getQueryId());
            jgen.writeStringField(SOURCE_TYPE_FIELD, pingResponse.getSourceType());
            jgen.writeNumberField(CAPABILITIES_FIELD, pingResponse.getCapabilities());
            jgen.writeNumberField(SERDE_VERSION_FIELD, pingResponse.getSerDeVersion());
            // new fields should only be appended to the end for forwards compatibility
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        public Deserializer()
        {
            super(FederationResponse.class, PingResponse.class);
        }

        @Override
        public FederationResponse deserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            validateObjectStart(jparser.getCurrentToken());
            return doDeserialize(jparser, ctxt);
            // do not validate object end to allow forwards compatibility
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
            String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
            String sourceType = getNextStringField(jparser, SOURCE_TYPE_FIELD);
            int capabilities = getNextIntField(jparser, CAPABILITIES_FIELD);
            int serDeVersion;
            try {
                serDeVersion = getNextIntField(jparser, SERDE_VERSION_FIELD);
            }
            catch (IllegalStateException e) {
                // this is for backwards compatibility as older SDK versions don't return this field
                serDeVersion = 1;
            }

            return new PingResponse(catalogName, queryId, sourceType, capabilities, serDeVersion);
        }
    }
}
