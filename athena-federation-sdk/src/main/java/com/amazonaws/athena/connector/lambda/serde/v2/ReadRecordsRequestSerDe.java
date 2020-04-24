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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class ReadRecordsRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String SPLIT_FIELD = "split";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String MAX_BLOCK_SIZE_FIELD = "maxBlockSize";
    private static final String MAX_INLINE_BLOCK_SIZE_FIELD = "maxInlineBlockSize";

    private ReadRecordsRequestSerDe(){}

    static final class Serializer extends TypedSerializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final TableNameSerDe.Serializer tableNameSerializer;
        private final ConstraintsSerDe.Serializer constraintsSerializer;
        private final SchemaSerDe.Serializer schemaSerializer;
        private final SplitSerDe.Serializer splitSerializer;

        Serializer(
                FederatedIdentitySerDe.Serializer identitySerializer,
                TableNameSerDe.Serializer tableNameSerializer,
                ConstraintsSerDe.Serializer constraintsSerializer,
                SchemaSerDe.Serializer schemaSerializer,
                SplitSerDe.Serializer splitSerializer)
        {
            super(FederationRequest.class, ReadRecordsRequest.class);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
            this.constraintsSerializer = requireNonNull(constraintsSerializer, "constraintsSerializer is null");
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
            this.splitSerializer = requireNonNull(splitSerializer, "splitSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ReadRecordsRequest readRecordsRequest = (ReadRecordsRequest) federationRequest;

            jgen.writeFieldName(IDENTITY_FIELD);
            identitySerializer.serialize(readRecordsRequest.getIdentity(), jgen, provider);

            jgen.writeStringField(QUERY_ID_FIELD, readRecordsRequest.getQueryId());
            jgen.writeStringField(CATALOG_NAME_FIELD, readRecordsRequest.getCatalogName());

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(readRecordsRequest.getTableName(), jgen, provider);

            jgen.writeFieldName(SCHEMA_FIELD);
            schemaSerializer.serialize(readRecordsRequest.getSchema(), jgen, provider);

            jgen.writeFieldName(SPLIT_FIELD);
            splitSerializer.serialize(readRecordsRequest.getSplit(), jgen, provider);

            jgen.writeFieldName(CONSTRAINTS_FIELD);
            constraintsSerializer.serialize(readRecordsRequest.getConstraints(), jgen, provider);

            jgen.writeStringField(MAX_BLOCK_SIZE_FIELD, String.valueOf(readRecordsRequest.getMaxBlockSize()));
            jgen.writeStringField(MAX_INLINE_BLOCK_SIZE_FIELD, String.valueOf(readRecordsRequest.getMaxInlineBlockSize()));
        }
    }

    static final class Deserializer extends TypedDeserializer<FederationRequest>
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final TableNameSerDe.Deserializer tableNameDeserializer;
        private final ConstraintsSerDe.Deserializer constraintsDeserializer;
        private final SchemaSerDe.Deserializer schemaDeserializer;
        private final SplitSerDe.Deserializer splitDeserializer;

        Deserializer(
                FederatedIdentitySerDe.Deserializer identityDeserializer,
                TableNameSerDe.Deserializer tableNameDeserializer,
                ConstraintsSerDe.Deserializer constraintsDeserializer,
                SchemaSerDe.Deserializer schemaDeserializer,
                SplitSerDe.Deserializer splitDeserializer)
        {
            super(FederationRequest.class, ReadRecordsRequest.class);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
            this.constraintsDeserializer = requireNonNull(constraintsDeserializer, "constraintsDeserializer is null");
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
            this.splitDeserializer = requireNonNull(splitDeserializer, "splitDeserializer is null");
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

            assertFieldName(jparser, SCHEMA_FIELD);
            Schema schema = schemaDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, SPLIT_FIELD);
            Split split = splitDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, CONSTRAINTS_FIELD);
            Constraints constraints = constraintsDeserializer.deserialize(jparser, ctxt);

            long maxBlockSize = Long.parseLong(getNextStringField(jparser, MAX_BLOCK_SIZE_FIELD));
            long maxInlineBlockSize = Long.parseLong(getNextStringField(jparser, MAX_INLINE_BLOCK_SIZE_FIELD));

            return new ReadRecordsRequest(identity, catalogName, queryId, tableName, schema, split, constraints, maxBlockSize, maxInlineBlockSize);
        }
    }
}
