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
package com.amazonaws.athena.connector.lambda.serde.v24;

import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class RemoteReadRecordsResponseSerDe extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String REMOTE_BLOCKS_FIELD = "remoteBlocks";
    private static final String ENCRYPTION_KEY_FIELD = "encryptionKey";

    private final SchemaSerDe schemaSerDe;
    private final SpillLocationSerDe spillLocationSerDe;
    private final EncryptionKeySerDe encryptionKeySerDe;

    public RemoteReadRecordsResponseSerDe(SchemaSerDe schemaSerDe, SpillLocationSerDe spillLocationSerDe, EncryptionKeySerDe encryptionKeySerDe)
    {
        this.schemaSerDe = requireNonNull(schemaSerDe, "schemaSerDe is null");
        this.spillLocationSerDe = requireNonNull(spillLocationSerDe, "spillLocationSerDe is null");
        this.encryptionKeySerDe = requireNonNull(encryptionKeySerDe, "encryptionKeySerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        RemoteReadRecordsResponse remoteReadRecordsResponse = (RemoteReadRecordsResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, remoteReadRecordsResponse.getCatalogName());

        jgen.writeFieldName(SCHEMA_FIELD);
        schemaSerDe.serialize(jgen, remoteReadRecordsResponse.getSchema());

        jgen.writeArrayFieldStart(REMOTE_BLOCKS_FIELD);
        for (SpillLocation spillLocation : remoteReadRecordsResponse.getRemoteBlocks()) {
            spillLocationSerDe.serialize(jgen, spillLocation);
        }
        jgen.writeEndArray();

        jgen.writeFieldName(ENCRYPTION_KEY_FIELD);
        encryptionKeySerDe.serialize(jgen, remoteReadRecordsResponse.getEncryptionKey());
    }

    @Override
    public RemoteReadRecordsResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, SCHEMA_FIELD);
        Schema schema = schemaSerDe.deserialize(jparser);

        assertFieldName(jparser, REMOTE_BLOCKS_FIELD);
        validateArrayStart(jparser);
        ImmutableList.Builder<SpillLocation> remoteBlocksList = ImmutableList.builder();
        while (jparser.nextToken() != JsonToken.END_ARRAY) {
            validateObjectStart(jparser.getCurrentToken());
            remoteBlocksList.add(spillLocationSerDe.doDeserialize(jparser));
            validateObjectEnd(jparser);
        }

        assertFieldName(jparser, ENCRYPTION_KEY_FIELD);
        EncryptionKey encryptionKey = encryptionKeySerDe.deserialize(jparser);

        return new RemoteReadRecordsResponse(catalogName, schema, remoteBlocksList.build(), encryptionKey);
    }
}
