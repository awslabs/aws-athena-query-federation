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

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class RemoteReadRecordsResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String REMOTE_BLOCKS_FIELD = "remoteBlocks";
    private static final String ENCRYPTION_KEY_FIELD = "encryptionKey";

    private RemoteReadRecordsResponseSerDe(){}

    static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final SchemaSerDe.Serializer schemaSerializer;
        private final SpillLocationSerDe.Serializer spillLocationSerializer;
        private final EncryptionKeySerDe.Serializer encryptionKeySerializer;

        Serializer(
                SchemaSerDe.Serializer schemaSerializer,
                SpillLocationSerDe.Serializer spillLocationSerializer,
                EncryptionKeySerDe.Serializer encryptionKeySerializer)
        {
            super(FederationResponse.class, RemoteReadRecordsResponse.class);
            this.schemaSerializer = requireNonNull(schemaSerializer, "schemaSerializer is null");
            this.spillLocationSerializer = requireNonNull(spillLocationSerializer, "spillLocationSerializer is null");
            this.encryptionKeySerializer = requireNonNull(encryptionKeySerializer, "encryptionKeySerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            RemoteReadRecordsResponse remoteReadRecordsResponse = (RemoteReadRecordsResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, remoteReadRecordsResponse.getCatalogName());

            jgen.writeFieldName(SCHEMA_FIELD);
            schemaSerializer.serialize(remoteReadRecordsResponse.getSchema(), jgen, provider);

            jgen.writeArrayFieldStart(REMOTE_BLOCKS_FIELD);
            for (SpillLocation spillLocation : remoteReadRecordsResponse.getRemoteBlocks()) {
                spillLocationSerializer.serialize(spillLocation, jgen, provider);
            }
            jgen.writeEndArray();

            jgen.writeFieldName(ENCRYPTION_KEY_FIELD);
            encryptionKeySerializer.serialize(remoteReadRecordsResponse.getEncryptionKey(), jgen, provider);
        }
    }

    static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final SchemaSerDe.Deserializer schemaDeserializer;
        private final SpillLocationSerDe.Deserializer spillLocationDeserializer;
        private final EncryptionKeySerDe.Deserializer encryptionKeyDeserializer;

        Deserializer(
                SchemaSerDe.Deserializer schemaDeserializer,
                SpillLocationSerDe.Deserializer spillLocationDeserializer,
                EncryptionKeySerDe.Deserializer encryptionKeyDeserializer)
        {
            super(FederationResponse.class, RemoteReadRecordsResponse.class);
            this.schemaDeserializer = requireNonNull(schemaDeserializer, "schemaDeserializer is null");
            this.spillLocationDeserializer = requireNonNull(spillLocationDeserializer, "spillLocationDeserializer is null");
            this.encryptionKeyDeserializer = requireNonNull(encryptionKeyDeserializer, "encryptionKeyDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, SCHEMA_FIELD);
            Schema schema = schemaDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, REMOTE_BLOCKS_FIELD);
            validateArrayStart(jparser);
            ImmutableList.Builder<SpillLocation> remoteBlocksList = ImmutableList.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                remoteBlocksList.add(spillLocationDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            assertFieldName(jparser, ENCRYPTION_KEY_FIELD);
            EncryptionKey encryptionKey = encryptionKeyDeserializer.deserialize(jparser, ctxt);

            return new RemoteReadRecordsResponse(catalogName, schema, remoteBlocksList.build(), encryptionKey);
        }
    }
}
