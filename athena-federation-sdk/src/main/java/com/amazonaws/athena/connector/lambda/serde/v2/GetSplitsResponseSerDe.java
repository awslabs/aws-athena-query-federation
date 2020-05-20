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
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

final class GetSplitsResponseSerDe
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SPLITS_FIELD = "splits";
    private static final String CONTINUATION_TOKEN_FIELD = "continuationToken";

    private GetSplitsResponseSerDe(){}

    static final class Serializer extends TypedSerializer<FederationResponse>
    {
        private final SplitSerDe.Serializer splitSerializer;

        Serializer(SplitSerDe.Serializer splitSerializer)
        {
            super(FederationResponse.class, GetSplitsResponse.class);
            this.splitSerializer = requireNonNull(splitSerializer, "splitSerializer is null");
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetSplitsResponse getSplitsResponse = (GetSplitsResponse) federationResponse;

            jgen.writeStringField(CATALOG_NAME_FIELD, getSplitsResponse.getCatalogName());

            jgen.writeArrayFieldStart(SPLITS_FIELD);
            for (Split split : getSplitsResponse.getSplits()) {
                splitSerializer.serialize(split, jgen, provider);
            }
            jgen.writeEndArray();

            jgen.writeStringField(CONTINUATION_TOKEN_FIELD, getSplitsResponse.getContinuationToken());
        }
    }

    static final class Deserializer extends TypedDeserializer<FederationResponse>
    {
        private final SplitSerDe.Deserializer splitDeserializer;

        Deserializer(SplitSerDe.Deserializer splitDeserializer)
        {
            super(FederationResponse.class, GetSplitsResponse.class);
            this.splitDeserializer = requireNonNull(splitDeserializer, "splitDeserializer is null");
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

            assertFieldName(jparser, SPLITS_FIELD);
            validateArrayStart(jparser);
            ImmutableSet.Builder<Split> splitsSet = ImmutableSet.builder();
            while (jparser.nextToken() != JsonToken.END_ARRAY) {
                validateObjectStart(jparser.getCurrentToken());
                splitsSet.add(splitDeserializer.doDeserialize(jparser, ctxt));
                validateObjectEnd(jparser);
            }

            String continuationToken = getNextStringField(jparser, CONTINUATION_TOKEN_FIELD);

            return new GetSplitsResponse(catalogName, splitsSet.build(), continuationToken);
        }
    }
}
