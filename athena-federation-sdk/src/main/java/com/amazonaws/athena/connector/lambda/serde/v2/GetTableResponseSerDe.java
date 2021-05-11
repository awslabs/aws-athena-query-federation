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
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class GetTableResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String PARTITION_COLUMNS_FIELD = "partitionColumns";

    private GetTableResponseSerDe(){}

    public static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final TableNameSerDe.Serializer tableNameSerializer;
        private final VersionedSerDe.Serializer<Schema> schemaSerializer;

        public Serializer(TableNameSerDe.Serializer tableNameSerializer, VersionedSerDe.Serializer<Schema> schemaSerializer)
        {
            super(FederationResponse.class, GetTableResponse.class);
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetTableResponse getTableResponse = (GetTableResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, getTableResponse.getCatalogName());

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(getTableResponse.getTableName(), jgen, provider);

            jgen.writeFieldName(SCHEMA_FIELD);
            schemaSerializer.serialize(getTableResponse.getSchema(), jgen, provider);

            writeStringArray(jgen, PARTITION_COLUMNS_FIELD, getTableResponse.getPartitionColumns());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final TableNameSerDe.Deserializer tableNameDeserializer;
        private final VersionedSerDe.Deserializer<Schema> schemaDeserializer;

        public Deserializer(TableNameSerDe.Deserializer tableNameDeserializer, VersionedSerDe.Deserializer<Schema> schemaDeserializer)
        {
            super(FederationResponse.class, GetTableResponse.class);
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, SCHEMA_FIELD);
            Schema schema = schemaDeserializer.deserialize(jparser, ctxt);

            ImmutableSet.Builder<String> partitionColsSet = ImmutableSet.builder();
            partitionColsSet.addAll(getNextStringArray(jparser, PARTITION_COLUMNS_FIELD));

            return new GetTableResponse(catalogName, tableName, schema, partitionColsSet.build());
        }
    }
}
