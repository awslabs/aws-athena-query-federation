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
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class GetTableResponseSerDe
        extends TypedSerDe<FederationResponse>
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String PARTITION_COLUMNS_FIELD = "partitionColumns";

    private final TableNameSerDe tableNameSerDe;
    private final SchemaSerDe schemaSerDe;

    public GetTableResponseSerDe(TableNameSerDe tableNameSerDe, SchemaSerDe schemaSerDe)
    {
        super(GetTableResponse.class);
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "tableNameSerDe is null");
        this.schemaSerDe = requireNonNull(schemaSerDe, "schemaSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationResponse federationResponse)
            throws IOException
    {
        GetTableResponse getTableResponse = (GetTableResponse) federationResponse;

        jgen.writeStringField(CATALOG_NAME_FIELD, getTableResponse.getCatalogName());

        jgen.writeFieldName(TABLE_NAME_FIELD);
        tableNameSerDe.serialize(jgen, getTableResponse.getTableName());

        jgen.writeFieldName(SCHEMA_FIELD);
        schemaSerDe.serialize(jgen, getTableResponse.getSchema());

        writeStringArray(jgen, PARTITION_COLUMNS_FIELD, getTableResponse.getPartitionColumns());
    }

    @Override
    public GetTableResponse doDeserialize(JsonParser jparser)
            throws IOException
    {
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, TABLE_NAME_FIELD);
        TableName tableName = tableNameSerDe.deserialize(jparser);

        assertFieldName(jparser, SCHEMA_FIELD);
        Schema schema = schemaSerDe.deserialize(jparser);

        ImmutableSet.Builder<String> partitionColsSet = ImmutableSet.builder();
        partitionColsSet.addAll(getNextStringArray(jparser, PARTITION_COLUMNS_FIELD));

        return new GetTableResponse(catalogName, tableName, schema, partitionColsSet.build());
    }
}
