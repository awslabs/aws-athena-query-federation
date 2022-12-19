/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.serde.v3;

import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetDataSourceCapabilitiesResponseSerDeV3
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String CAPABILITIES_NAME_FIELD = "capabilities";

    private GetDataSourceCapabilitiesResponseSerDeV3() {}

    public static final class Serializer extends TypedSerializer<FederationResponse> implements VersionedSerDe.Serializer<FederationResponse>
    {
        public Serializer()
        {
            super(FederationResponse.class, GetDataSourceCapabilitiesResponse.class);
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetDataSourceCapabilitiesResponse getDataSourceCapabilitiesResponse = (GetDataSourceCapabilitiesResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, getDataSourceCapabilitiesResponse.getCatalogName());

            jgen.writeObjectFieldStart(CAPABILITIES_NAME_FIELD);
            for (Map.Entry<String, List<String>> entry : getDataSourceCapabilitiesResponse.getCapabilities().entrySet()) {
                writeStringArray(jgen, entry.getKey(), entry.getValue());
            }
            jgen.writeEndObject();
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse> implements VersionedSerDe.Deserializer<FederationResponse>
    {
        public Deserializer()
        {
            super(FederationResponse.class, GetDataSourceCapabilitiesResponse.class);
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, CAPABILITIES_NAME_FIELD);
            jparser.nextToken();

            ImmutableMap.Builder<String, List<String>> capabilities = ImmutableMap.builder();
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                String field = jparser.getCurrentName();
                validateArrayStart(jparser);
                List<String> subTypes = new ArrayList<>();
                while (jparser.nextToken() != JsonToken.END_ARRAY) {
                    subTypes.add(jparser.getValueAsString());
                }
                capabilities.put(field, subTypes);
            }

            return new GetDataSourceCapabilitiesResponse(catalogName, capabilities.build());
        }
    }
}
