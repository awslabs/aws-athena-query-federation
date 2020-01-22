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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ReadRecordsRequestSerDe
        extends TypedSerDe<FederationRequest>
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String SCHEMA_FIELD = "schema";
    private static final String SPLIT_FIELD = "split";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String MAX_BLOCK_SIZE_FIELD = "maxBlockSize";
    private static final String MAX_INLINE_BLOCK_SIZE_FIELD = "maxInlineBlockSize";

    private final FederatedIdentitySerDe federatedIdentitySerDe;
    private final TableNameSerDe tableNameSerDe;
    private final ConstraintsSerDe constraintsSerDe;
    private final SchemaSerDe schemaSerDe;
    private final SplitSerDe splitSerDe;

    public ReadRecordsRequestSerDe(FederatedIdentitySerDe federatedIdentitySerDe, TableNameSerDe tableNameSerDe, ConstraintsSerDe constraintsSerDe, SchemaSerDe schemaSerDe,
            SplitSerDe splitSerDe)
    {
        this.federatedIdentitySerDe = requireNonNull(federatedIdentitySerDe, "federatedIdentitySerDe is null");
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "tableNameSerDe is null");
        this.constraintsSerDe = requireNonNull(constraintsSerDe, "constraintsSerDe is null");
        this.schemaSerDe = requireNonNull(schemaSerDe, "schemaSerDe is null");
        this.splitSerDe = requireNonNull(splitSerDe, "splitSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationRequest federationRequest)
            throws IOException
    {
        ReadRecordsRequest readRecordsRequest = (ReadRecordsRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        federatedIdentitySerDe.serialize(jgen, readRecordsRequest.getIdentity());

        jgen.writeStringField(QUERY_ID_FIELD, readRecordsRequest.getQueryId());
        jgen.writeStringField(CATALOG_NAME_FIELD, readRecordsRequest.getCatalogName());

        jgen.writeFieldName(TABLE_NAME_FIELD);
        tableNameSerDe.serialize(jgen, readRecordsRequest.getTableName());

        jgen.writeFieldName(SCHEMA_FIELD);
        schemaSerDe.serialize(jgen, readRecordsRequest.getSchema());

        jgen.writeFieldName(SPLIT_FIELD);
        splitSerDe.serialize(jgen, readRecordsRequest.getSplit());

        jgen.writeFieldName(CONSTRAINTS_FIELD);
        constraintsSerDe.serialize(jgen, readRecordsRequest.getConstraints());

        jgen.writeStringField(MAX_BLOCK_SIZE_FIELD, String.valueOf(readRecordsRequest.getMaxBlockSize()));
        jgen.writeStringField(MAX_INLINE_BLOCK_SIZE_FIELD, String.valueOf(readRecordsRequest.getMaxInlineBlockSize()));
    }

    @Override
    public ReadRecordsRequest doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity identity = federatedIdentitySerDe.deserialize(jparser);

        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, TABLE_NAME_FIELD);
        TableName tableName = tableNameSerDe.deserialize(jparser);

        assertFieldName(jparser, SCHEMA_FIELD);
        Schema schema = schemaSerDe.deserialize(jparser);

        assertFieldName(jparser, SPLIT_FIELD);
        Split split = splitSerDe.deserialize(jparser);

        assertFieldName(jparser, CONSTRAINTS_FIELD);
        Constraints constraints = constraintsSerDe.deserialize(jparser);

        long maxBlockSize = Long.parseLong(getNextStringField(jparser, MAX_BLOCK_SIZE_FIELD));
        long maxInlineBlockSize = Long.parseLong(getNextStringField(jparser, MAX_INLINE_BLOCK_SIZE_FIELD));

        return new ReadRecordsRequest(identity, catalogName, queryId, tableName, schema, split, constraints, maxBlockSize, maxInlineBlockSize);
    }
}
