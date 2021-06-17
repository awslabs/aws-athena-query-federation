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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class ListTablesResponseSerDe
{
    private static final String TABLES_FIELD = "tables";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String NEXT_TOKEN_FIELD = "nextToken";

    private ListTablesResponseSerDe(){}

    public static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final TableNameSerDe.Serializer tableNameSerializer;

        public Serializer(TableNameSerDe.Serializer tableNameSerializer)
        {
            super(FederationResponse.class, ListTablesResponse.class);
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ListTablesResponse listTablesResponse = (ListTablesResponse) federationResponse;

            jgen.writeArrayFieldStart(TABLES_FIELD);
            for (TableName tableName : listTablesResponse.getTables()) {
                tableNameSerializer.serialize(tableName, jgen, provider);
            }
            jgen.writeEndArray();
            jgen.writeStringField(CATALOG_NAME_FIELD, listTablesResponse.getCatalogName());
            jgen.writeStringField(NEXT_TOKEN_FIELD, listTablesResponse.getNextToken());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final TableNameSerDe.Deserializer tableNameDeserializer;

        public Deserializer(TableNameSerDe.Deserializer tableNameDeserializer)
        {
            super(FederationResponse.class, ListTablesResponse.class);
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, TABLES_FIELD);
            ImmutableList.Builder<TableName> tablesList = ImmutableList.builder();
            validateArrayStart(jparser);
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                tablesList.add(tableNameDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            /**
             * TODO: This logic must be modified in V3 of the SDK to enforce the presence of nextToken in the JSON
             *       contract.
             *       For backwards compatibility with V2 of the SDK, we will first verify that the JSON contract
             *       contains the nextToken argument, and if not, set the default value for it.
             */
            String nextToken = null;
            if (!JsonToken.END_OBJECT.equals(jparser.nextToken()) &&
                    jparser.getCurrentName().equals(NEXT_TOKEN_FIELD)) {
                jparser.nextToken();
                nextToken = jparser.getValueAsString();
            }

            return new ListTablesResponse(catalogName, tablesList.build(), nextToken);
        }
    }
}
