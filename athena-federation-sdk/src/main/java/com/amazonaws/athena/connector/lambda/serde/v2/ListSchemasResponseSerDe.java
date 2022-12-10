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

import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Collection;

public final class ListSchemasResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMAS_FIELD = "schemas";

    private ListSchemasResponseSerDe() {}

    public static final class Serializer extends TypedSerializer<FederationResponse>
    {
        public Serializer()
        {
            super(FederationResponse.class, ListSchemasResponse.class);
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ListSchemasResponse listSchemasResponse = (ListSchemasResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, listSchemasResponse.getCatalogName());

            writeStringArray(jgen, SCHEMAS_FIELD, listSchemasResponse.getSchemas());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        public Deserializer()
        {
            super(FederationResponse.class, ListSchemasResponse.class);
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
            Collection<String> schemas = getNextStringArray(jparser, SCHEMAS_FIELD);

            return new ListSchemasResponse(catalogName, schemas);
        }
    }
}
