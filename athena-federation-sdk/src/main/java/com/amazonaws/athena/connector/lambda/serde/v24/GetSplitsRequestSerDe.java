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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.TypedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class GetSplitsRequestSerDe
        extends TypedSerDe<FederationRequest>
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String PARTITIONS_FIELD = "partitions";
    private static final String PARTITION_COLS_FIELD = "partitionColumns";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String CONTINUATION_TOKEN_FIELD = "continuationToken";

    private final FederatedIdentitySerDe federatedIdentitySerDe;
    private final TableNameSerDe tableNameSerDe;
    private final BlockSerDe blockSerDe;
    private final ConstraintsSerDe constraintsSerDe;

    public GetSplitsRequestSerDe(FederatedIdentitySerDe federatedIdentitySerDe, TableNameSerDe tableNameSerDe, BlockSerDe blockSerDe, ConstraintsSerDe constraintsSerDe)
    {
        super(GetSplitsRequest.class);
        this.federatedIdentitySerDe = requireNonNull(federatedIdentitySerDe, "federatedIdentitySerDe is null");
        this.tableNameSerDe = requireNonNull(tableNameSerDe, "tableNameSerDe is null");
        this.blockSerDe = requireNonNull(blockSerDe, "blockSerDe is null");
        this.constraintsSerDe = requireNonNull(constraintsSerDe, "constraintsSerDe is null");
    }

    @Override
    public void doSerialize(JsonGenerator jgen, FederationRequest federationRequest)
            throws IOException
    {
        GetSplitsRequest getSplitsRequest = (GetSplitsRequest) federationRequest;

        jgen.writeFieldName(IDENTITY_FIELD);
        federatedIdentitySerDe.serialize(jgen, getSplitsRequest.getIdentity());

        jgen.writeStringField(QUERY_ID_FIELD, getSplitsRequest.getQueryId());
        jgen.writeStringField(CATALOG_NAME_FIELD, getSplitsRequest.getCatalogName());

        jgen.writeFieldName(TABLE_NAME_FIELD);
        tableNameSerDe.serialize(jgen, getSplitsRequest.getTableName());

        jgen.writeFieldName(PARTITIONS_FIELD);
        blockSerDe.serialize(jgen, getSplitsRequest.getPartitions());

        writeStringArray(jgen, PARTITION_COLS_FIELD, getSplitsRequest.getPartitionCols());

        jgen.writeFieldName(CONSTRAINTS_FIELD);
        constraintsSerDe.serialize(jgen, getSplitsRequest.getConstraints());

        jgen.writeStringField(CONTINUATION_TOKEN_FIELD, getSplitsRequest.getContinuationToken());
    }

    @Override
    public GetSplitsRequest doDeserialize(JsonParser jparser)
            throws IOException
    {
        assertFieldName(jparser, IDENTITY_FIELD);
        FederatedIdentity identity = federatedIdentitySerDe.deserialize(jparser);

        String queryId = getNextStringField(jparser, QUERY_ID_FIELD);
        String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);

        assertFieldName(jparser, TABLE_NAME_FIELD);
        TableName tableName = tableNameSerDe.deserialize(jparser);

        assertFieldName(jparser, PARTITIONS_FIELD);
        Block partitions = blockSerDe.deserialize(jparser);

        List<String> partitionColumns = getNextStringArray(jparser, PARTITION_COLS_FIELD);

        assertFieldName(jparser, CONSTRAINTS_FIELD);
        Constraints constraints = constraintsSerDe.deserialize(jparser);

        String continuationToken = getNextStringField(jparser, CONTINUATION_TOKEN_FIELD);

        return new GetSplitsRequest(identity, queryId, catalogName, tableName, partitions, partitionColumns, constraints, continuationToken);
    }
}
