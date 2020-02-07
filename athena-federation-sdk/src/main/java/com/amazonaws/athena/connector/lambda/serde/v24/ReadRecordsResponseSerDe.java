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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ReadRecordsResponseSerDe extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String RECORDS_FIELD = "records";

    private final BlockSerDe blockSerDe;

    public ReadRecordsResponseSerDe(BlockSerDe blockSerDe)
    {
        super(ReadRecordsResponse.class);
        this.blockSerDe = requireNonNull(blockSerDe, "blockSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        ReadRecordsResponse readRecordsResponse = (ReadRecordsResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, readRecordsResponse.getCatalogName());

        jgen.writeFieldName(RECORDS_FIELD);
        blockSerDe.serialize(jgen, readRecordsResponse.getRecords());
    }

    @Override
    public ReadRecordsResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, RECORDS_FIELD);
        Block records = blockSerDe.deserialize(jparser);

        return new ReadRecordsResponse(catalogName, records);
    }
}
