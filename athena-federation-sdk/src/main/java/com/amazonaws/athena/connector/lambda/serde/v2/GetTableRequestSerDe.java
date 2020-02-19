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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class GetTableRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";

    private GetTableRequestSerDe(){}

    static final class Serializer extends TypedSerializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final TableNameSerDe.Serializer tableNameSerializer;

        Serializer(FederatedIdentitySerDe.Serializer identitySerializer, TableNameSerDe.Serializer tableNameSerializer)
        {
            super(FederationRequest.class, GetTableRequest.class);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetTableRequest getTableRequest = (GetTableRequest) federationRequest;

            jgen.writeFieldName(IDENTITY_FIELD);
            identitySerializer.serialize(getTableRequest.getIdentity(), jgen, provider);

            jgen.writeStringField(QUERY_ID_FIELD, getTableRequest.getQueryId());
            jgen.writeStringField(CATALOG_NAME_FIELD, getTableRequest.getCatalogName());

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(getTableRequest.getTableName(), jgen, provider);
        }
    }

    static final class Deserializer extends TypedDeserializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final TableNameSerDe.Deserializer tableNameDeserializer;

        Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer, TableNameSerDe.Deserializer tableNameDeserializer)
        {
            super(FederationRequest.class, GetTableRequest.class);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
        }

        @Override
        protected FederationRequest doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            assertFieldName(jparser, IDENTITY_FIELD);
            FederatedIdentity identity = identityDeserializer.deserialize(jparser, ctxt);

            String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            return new GetTableRequest(identity, queryId, catalogName, tableName);
        }
    }
}
