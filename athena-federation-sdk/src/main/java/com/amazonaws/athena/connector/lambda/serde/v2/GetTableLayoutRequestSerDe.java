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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class GetTableLayoutRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String SCHEMA_FIELD = "schema";
    private static final String PARTITION_COLS_FIELD = "partitionColumns";

    private GetTableLayoutRequestSerDe() {}

    public static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final TableNameSerDe.Serializer tableNameSerializer;
        private final VersionedSerDe.Serializer<Constraints> constraintsSerializer;
        private final VersionedSerDe.Serializer<Schema> schemaSerializer;

        public Serializer(
                FederatedIdentitySerDe.Serializer identitySerializer,
                TableNameSerDe.Serializer tableNameSerializer,
                VersionedSerDe.Serializer<Constraints> constraintsSerializer,
                VersionedSerDe.Serializer<Schema> schemaSerializer)
        {
            super(GetTableLayoutRequest.class, identitySerializer);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
            this.constraintsSerializer = requireNonNull(constraintsSerializer, "constraintsSerializer is null");
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
        }

        @Override
        protected void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetTableLayoutRequest getTableLayoutRequest = (GetTableLayoutRequest) federationRequest;

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(getTableLayoutRequest.getTableName(), jgen, provider);

            jgen.writeFieldName(CONSTRAINTS_FIELD);
            constraintsSerializer.serialize(getTableLayoutRequest.getConstraints(), jgen, provider);

            jgen.writeFieldName(SCHEMA_FIELD);
            schemaSerializer.serialize(getTableLayoutRequest.getSchema(), jgen, provider);

            writeStringArray(jgen, PARTITION_COLS_FIELD, getTableLayoutRequest.getPartitionCols());
        }
    }

    public static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final TableNameSerDe.Deserializer tableNameDeserializer;
        private final VersionedSerDe.Deserializer<Constraints> constraintsDeserializer;
        private final VersionedSerDe.Deserializer<Schema> schemaDeserializer;

        public Deserializer(
                FederatedIdentitySerDe.Deserializer identityDeserializer,
                TableNameSerDe.Deserializer tableNameDeserializer,
                VersionedSerDe.Deserializer<Constraints> constraintsDeserializer,
                VersionedSerDe.Deserializer<Schema> schemaDeserializer)
        {
            super(GetTableLayoutRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
            this.constraintsDeserializer = requireNonNull(constraintsDeserializer, "constraintsDeserializer is null");
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, CONSTRAINTS_FIELD);
            Constraints constraints = constraintsDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, SCHEMA_FIELD);
            Schema schema = schemaDeserializer.deserialize(jparser, ctxt);

            ImmutableSet.Builder<String> partitionColsSet = ImmutableSet.builder();
            partitionColsSet.addAll(getNextStringArray(jparser, PARTITION_COLS_FIELD));

            return new GetTableLayoutRequest(identity, queryId, catalogName, tableName, constraints, schema, partitionColsSet.build());
        }
    }
}
