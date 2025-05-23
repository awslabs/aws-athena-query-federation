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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.serde.FederatedIdentitySerDe;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class GetSplitsRequestSerDe
{
    private static final String IDENTITY_FIELD = "identity";
    private static final String QUERY_ID_FIELD = "queryId";
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String TABLE_NAME_FIELD = "tableName";
    private static final String PARTITIONS_FIELD = "partitions";
    private static final String PARTITION_COLS_FIELD = "partitionColumns";
    private static final String CONSTRAINTS_FIELD = "constraints";
    private static final String CONTINUATION_TOKEN_FIELD = "continuationToken";

    private GetSplitsRequestSerDe() {}

    public static final class Serializer extends MetadataRequestSerializer
    {
        private final FederatedIdentitySerDe.Serializer identitySerializer;
        private final TableNameSerDe.Serializer tableNameSerializer;
        private final VersionedSerDe.Serializer<Block> blockSerializer;
        private final VersionedSerDe.Serializer<Constraints> constraintsSerializer;

        public Serializer(
                FederatedIdentitySerDe.Serializer identitySerializer,
                TableNameSerDe.Serializer tableNameSerializer,
                VersionedSerDe.Serializer<Block> blockSerializer,
                VersionedSerDe.Serializer<Constraints> constraintsSerializer)
        {
            super(GetSplitsRequest.class, identitySerializer);
            this.identitySerializer = requireNonNull(identitySerializer, "identitySerializer is null");
            this.tableNameSerializer = requireNonNull(tableNameSerializer, "tableNameSerializer is null");
            this.blockSerializer = requireNonNull(blockSerializer, "blockSerializer is null");
            this.constraintsSerializer = requireNonNull(constraintsSerializer, "constraintsSerializer is null");
        }

        @Override
        protected void doRequestSerialize(FederationRequest federationRequest, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetSplitsRequest getSplitsRequest = (GetSplitsRequest) federationRequest;

            jgen.writeFieldName(TABLE_NAME_FIELD);
            tableNameSerializer.serialize(getSplitsRequest.getTableName(), jgen, provider);

            jgen.writeFieldName(PARTITIONS_FIELD);
            blockSerializer.serialize(getSplitsRequest.getPartitions(), jgen, provider);

            writeStringArray(jgen, PARTITION_COLS_FIELD, getSplitsRequest.getPartitionCols());

            jgen.writeFieldName(CONSTRAINTS_FIELD);
            constraintsSerializer.serialize(getSplitsRequest.getConstraints(), jgen, provider);

            jgen.writeStringField(CONTINUATION_TOKEN_FIELD, getSplitsRequest.getContinuationToken());
        }
    }

    public static final class Deserializer extends MetadataRequestDeserializer
    {
        private final FederatedIdentitySerDe.Deserializer identityDeserializer;
        private final TableNameSerDe.Deserializer tableNameDeserializer;
        private final VersionedSerDe.Deserializer<Block> blockDeserializer;
        private final VersionedSerDe.Deserializer<Constraints> constraintsDeserializer;

        public Deserializer(
                FederatedIdentitySerDe.Deserializer identityDeserializer,
                TableNameSerDe.Deserializer tableNameDeserializer,
                VersionedSerDe.Deserializer<Block> blockDeserializer,
                VersionedSerDe.Deserializer<Constraints> constraintsDeserializer)
        {
            super(GetSplitsRequest.class, identityDeserializer);
            this.identityDeserializer = requireNonNull(identityDeserializer, "identityDeserializer is null");
            this.tableNameDeserializer = requireNonNull(tableNameDeserializer, "tableNameDeserializer is null");
            this.blockDeserializer = requireNonNull(blockDeserializer, "blockDeserializer is null");
            this.constraintsDeserializer = requireNonNull(constraintsDeserializer, "constraintsDeserializer is null");
        }

        @Override
        protected MetadataRequest doRequestDeserialize(JsonParser jparser, DeserializationContext ctxt, FederatedIdentity identity, String queryId, String catalogName)
                throws IOException
        {
            assertFieldName(jparser, TABLE_NAME_FIELD);
            TableName tableName = tableNameDeserializer.deserialize(jparser, ctxt);

            assertFieldName(jparser, PARTITIONS_FIELD);
            Block partitions = blockDeserializer.deserialize(jparser, ctxt);

            List<String> partitionColumns = getNextStringArray(jparser, PARTITION_COLS_FIELD);

            assertFieldName(jparser, CONSTRAINTS_FIELD);
            Constraints constraints = constraintsDeserializer.deserialize(jparser, ctxt);

            String continuationToken = getNextStringField(jparser, CONTINUATION_TOKEN_FIELD);

            return new GetSplitsRequest(identity, queryId, catalogName, tableName, partitions, partitionColumns, constraints, continuationToken);
        }
    }
}
