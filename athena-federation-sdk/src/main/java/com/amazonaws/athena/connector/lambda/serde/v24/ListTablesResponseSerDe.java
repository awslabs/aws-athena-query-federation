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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ListTablesResponseSerDe
        extends TypedSerDe<FederationResponse>
{
    private static final String TABLES_FIELD = "tables";
    private static final String CATALOG_NAME_FIELD = "catalogName";

    public final TableNameSerDe tableNameSerDe;

    public ListTablesResponseSerDe(TableNameSerDe tableNameSerDe)
    {
        super(ListTablesResponse.class);
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "tableNameSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        ListTablesResponse listTablesResponse = (ListTablesResponse) federationResponse;

        jgen.writeArrayFieldStart(TABLES_FIELD);
        for (TableName tableName : listTablesResponse.getTables()) {
            tableNameSerDe.serialize(jgen, tableName);
        }
        jgen.writeEndArray();

        jgen.writeStringField(CATALOG_NAME_FIELD, listTablesResponse.getCatalogName());
    }

    @Override
    public ListTablesResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, TABLES_FIELD);
        ImmutableList.Builder<TableName> tablesList = ImmutableList.builder();
        validateArrayStart(jparser);
        while (jparser.nextToken() != JsonToken.END_ARRAY) {
            validateObjectStart(jparser.getCurrentToken());
            tablesList.add(tableNameSerDe.doDeserialize(jparser));
            validateObjectEnd(jparser);
        }

        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        return new ListTablesResponse(catalogName, tablesList.build());
    }
}
