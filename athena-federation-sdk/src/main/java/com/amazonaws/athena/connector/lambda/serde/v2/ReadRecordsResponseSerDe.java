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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class ReadRecordsResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String RECORDS_FIELD = "records";

    private ReadRecordsResponseSerDe(){}

    static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final BlockSerDe.Serializer blockSerializer;

        Serializer(BlockSerDe.Serializer blockSerializer)
        {
            super(FederationResponse.class, ReadRecordsResponse.class);
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            ReadRecordsResponse readRecordsResponse = (ReadRecordsResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, readRecordsResponse.getCatalogName());

            jgen.writeFieldName(RECORDS_FIELD);
            blockSerializer.serialize(readRecordsResponse.getRecords(), jgen, provider);
        }
    }

    static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final BlockSerDe.Deserializer blockDeserializer;

        Deserializer(BlockSerDe.Deserializer blockDeserializer)
        {
            super(FederationResponse.class, ReadRecordsResponse.class);
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, RECORDS_FIELD);
            Block records = blockDeserializer.deserialize(jparser, ctxt);

            return new ReadRecordsResponse(catalogName, records);
        }
    }
}
