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
package com.amazonaws.athena.connector.lambda.serde.v5;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.v2.MetadataRequestDeserializer;
import com.amazonaws.athena.connector.lambda.serde.v2.MetadataRequestSerializer;
import com.amazonaws.athena.connector.lambda.serde.v2.TableNameSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class GetTableRequestSerDeV5
{
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String QUERY_PASSTHROUGH_ARGUMENTS = "queryPassthroughArguments";

    private GetTableRequestSerDeV5() {}

    public static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final TableNameSerDe.Serializer tableNameSerializer;

        public Serializer(FederatedIdentitySerDe.Serializer identitySerializer, TableNameSerDe.Serializer tableNameSerializer)
        {
            super(GetTableRequest.class, identitySerializer);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
        }

        @Override
        protected void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetTableRequest getTableRequest = (GetTableRequest) federationRequest;

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(getTableRequest.getTableName(), jgen, provider);
            writeStringMap(jgen, QUERY_PASSTHROUGH_ARGUMENTS, getTableRequest.getQueryPassthroughArguments());
        }
    }

    public static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final TableNameSerDe.Deserializer tableNameDeserializer;

        public Deserializer(FederatedIdentitySerDe.Deserializer identityDeserializer, TableNameSerDe.Deserializer tableNameDeserializer)
        {
            super(GetTableRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            Map<String, String> queryPassthroughArguments = new HashMap<>();
            assertFieldName(jparser, QUERY_PASSTHROUGH_ARGUMENTS);
            validateObjectStart(jparser.nextToken());
            while (jparser.nextToken() != JsonToken.END_OBJECT) {
                queryPassthroughArguments.put(jparser.getCurrentName(), jparser.getValueAsString());
            }

            GetTableRequest getTableRequest = new GetTableRequest(identity, queryId, catalogName, tableName, queryPassthroughArguments);
            return getTableRequest;
        }
    }
}
