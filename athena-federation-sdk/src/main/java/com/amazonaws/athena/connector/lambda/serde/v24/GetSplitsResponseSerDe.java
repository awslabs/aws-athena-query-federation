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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class GetSplitsResponseSerDe
        extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String SPLITS_FIELD = "splits";
    private static final String CONTINUATION_TOKEN_FIELD = "continuationToken";

    private final SplitSerDe splitSerDe;

    public GetSplitsResponseSerDe(SplitSerDe splitSerDe)
    {
        this.splitSerDe = requireNonNull(splitSerDe, "splitSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        GetSplitsResponse getSplitsResponse = (GetSplitsResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, getSplitsResponse.getCatalogName());

        jgen.writeArrayFieldStart(SPLITS_FIELD);
        for (Split split : getSplitsResponse.getSplits()) {
            splitSerDe.serialize(jgen, split);
        }
        jgen.writeEndArray();

        jgen.writeStringField(CONTINUATION_TOKEN_FIELD, getSplitsResponse.getContinuationToken());
    }

    @Override
    public GetSplitsResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, SPLITS_FIELD);
        validateArrayStart(jparser);
        ImmutableSet.Builder<Split> splitsSet = ImmutableSet.builder();
        while (jparser.nextToken() != JsonToken.END_ARRAY) {
            validateObjectStart(jparser.getCurrentToken());
            splitsSet.add(splitSerDe.doDeserialize(jparser));
            validateObjectEnd(jparser);
        }

        String continuationToken = getNextStringField(jparser, CONTINUATION_TOKEN_FIELD);

        return new GetSplitsResponse(catalogName, splitsSet.build(), continuationToken);
    }
}
