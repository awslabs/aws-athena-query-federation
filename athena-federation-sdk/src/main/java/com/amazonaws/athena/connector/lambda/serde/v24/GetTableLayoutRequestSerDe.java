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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class GetTableLayoutRequestSerDe
        extends TypedSerDe<FederationRequest>
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String SCHEMA_FIELD = "schema";
    private static final String PARTITION_COLS_FIELD = "partitionColumns";

    private final FederatedIdentitySerDe federatedIdentitySerDe;
    private final TableNameSerDe tableNameSerDe;
    private final ConstraintsSerDe constraintsSerDe;
    private final SchemaSerDe schemaSerDe;

    public GetTableLayoutRequestSerDe(FederatedIdentitySerDe federatedIdentitySerDe, TableNameSerDe tableNameSerDe, ConstraintsSerDe constraintsSerDe, SchemaSerDe schemaSerDe)
    {
        super(GetTableLayoutRequest.class);
        this.federatedIdentitySerDe = requireNonNull(federatedIdentitySerDe, "federatedIdentitySerDe is null");
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "tableNameSerDe is null");
        this.constraintsSerDe = requireNonNull(constraintsSerDe, "constraintsSerDe is null");
        this.schemaSerDe = requireNonNull(schemaSerDe, "schemaSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationRequest federationRequest)
            throws IOException
    {
        GetTableLayoutRequest getTableLayoutRequest = (GetTableLayoutRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        federatedIdentitySerDe.serialize(jgen, getTableLayoutRequest.getIdentity());

        jgen.writeStringField(QUERY_ID_FIELD, getTableLayoutRequest.getQueryId());
        jgen.writeStringField(CATALOG_NAME_FIELD, getTableLayoutRequest.getCatalogName());

        jgen.writeFieldName(TABLE_NAME_FIELD);
        tableNameSerDe.serialize(jgen, getTableLayoutRequest.getTableName());

        jgen.writeFieldName(CONSTRAINTS_FIELD);
        constraintsSerDe.serialize(jgen, getTableLayoutRequest.getConstraints());

        jgen.writeFieldName(SCHEMA_FIELD);
        schemaSerDe.serialize(jgen, getTableLayoutRequest.getSchema());

        writeStringArray(jgen, PARTITION_COLS_FIELD, getTableLayoutRequest.getPartitionCols());
    }

    @Override
    public GetTableLayoutRequest doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity identity = federatedIdentitySerDe.deserialize(jparser);

        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, TABLE_NAME_FIELD);
        TableName tableName = tableNameSerDe.deserialize(jparser);

        assertFieldName(jparser, CONSTRAINTS_FIELD);
        Constraints constraints = constraintsSerDe.deserialize(jparser);

        assertFieldName(jparser, SCHEMA_FIELD);
        Schema schema = schemaSerDe.deserialize(jparser);

        ImmutableSet.Builder<String> partitionColsSet = ImmutableSet.builder();
        partitionColsSet.addAll(getNextStringArray(jparser, PARTITION_COLS_FIELD));

        return new GetTableLayoutRequest(identity, queryId, catalogName, tableName, constraints, schema, partitionColsSet.build());
    }
}
