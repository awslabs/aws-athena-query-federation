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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

public class PingResponseSerDe extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String SOURCE_TYPE_FIELD = "sourceType";
    private static final String CAPABILITIES_FIELD = "capabilities";

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        PingResponse pingResponse = (PingResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, pingResponse.getCatalogName());
        jgen.writeStringField(QUERY_ID_FIELD, pingResponse.getQueryId());
        jgen.writeStringField(SOURCE_TYPE_FIELD, pingResponse.getSourceType());
        jgen.writeStringField(CAPABILITIES_FIELD, String.valueOf(pingResponse.getCapabilities()));
    }

    @Override
    public PingResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String sourceType = getNextStringField(jparser, SOURCE_TYPE_FIELD);
        int capabilities = Integer.parseInt(getNextStringField(jparser, CAPABILITIES_FIELD));

        return new PingResponse(catalogName, queryId, sourceType, capabilities);
    }
}
