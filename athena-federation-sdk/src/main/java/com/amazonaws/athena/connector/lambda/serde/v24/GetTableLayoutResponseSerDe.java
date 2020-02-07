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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class GetTableLayoutResponseSerDe
        extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String PARTITIONS_FIELD = "partitions";

    private final TableNameSerDe tableNameSerDe;
    private final BlockSerDe blockSerDe;

    public GetTableLayoutResponseSerDe(TableNameSerDe tableNameSerDe, BlockSerDe blockSerDe)
    {
        super(GetTableLayoutResponse.class);
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "requireNonNull is null");
        this.blockSerDe = requireNonNull(blockSerDe, "blockSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        GetTableLayoutResponse getTableLayoutResponse = (GetTableLayoutResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, getTableLayoutResponse.getCatalogName());

        jgen.writeFieldName(TABLE_NAME_FIELD);
        tableNameSerDe.serialize(jgen, getTableLayoutResponse.getTableName());

        jgen.writeFieldName(PARTITIONS_FIELD);
        blockSerDe.serialize(jgen, getTableLayoutResponse.getPartitions());
    }

    @Override
    public GetTableLayoutResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, TABLE_NAME_FIELD);
        TableName tableName = tableNameSerDe.deserialize(jparser);

        assertFieldName(jparser, PARTITIONS_FIELD);
        Block partitions = blockSerDe.deserialize(jparser);

        return new GetTableLayoutResponse(catalogName, tableName, partitions);
    }
}
