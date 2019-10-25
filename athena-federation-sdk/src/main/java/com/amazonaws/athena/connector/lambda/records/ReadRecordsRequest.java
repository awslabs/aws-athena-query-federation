package com.amazonaws.athena.connector.lambda.records;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import static java.util.Objects.requireNonNull;

public class ReadRecordsRequest
        extends RecordRequest
{
    private final TableName tableName;
    private final Schema schema;
    private final Split split;
    private final Constraints constraints;
    private final long maxBlockSize;
    private final long maxInlineBlockSize;

    public ReadRecordsRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("schema") Schema schema,
            @JsonProperty("split") Split split,
            @JsonProperty("constraints") Constraints constraints,
            @JsonProperty("maxBlockSize") long maxBlockSize,
            @JsonProperty("maxInlineBlockSize") long maxInlineBlockSize)
    {
        super(identity, RecordRequestType.READ_RECORDS, catalogName, queryId);
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(split, "split is null");
        requireNonNull(constraints, "constraints is null");
        this.schema = schema;
        this.tableName = tableName;
        this.split = split;
        this.maxBlockSize = maxBlockSize;
        this.maxInlineBlockSize = maxInlineBlockSize;
        this.constraints = constraints;
    }

    @JsonProperty
    public TableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Schema getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Split getSplit()
    {
        return split;
    }

    @JsonProperty
    public long getMaxInlineBlockSize()
    {
        return maxInlineBlockSize;
    }

    @JsonProperty
    public long getMaxBlockSize()
    {
        return maxBlockSize;
    }

    @JsonProperty
    public Constraints getConstraints()
    {
        return constraints;
    }

    @Override
    public void close()
            throws Exception
    {
        constraints.close();
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("tableName", tableName)
                .add("schema", schema)
                .add("split", split)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .add("maxBlockSize", maxBlockSize)
                .add("maxInlineBlockSize", maxInlineBlockSize)
                .add("constraints", constraints)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        ReadRecordsRequest that = (ReadRecordsRequest) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.schema, that.schema) &&
                Objects.equal(this.split, that.split) &&
                Objects.equal(this.constraints, that.constraints) &&
                Objects.equal(this.maxBlockSize, that.maxBlockSize) &&
                Objects.equal(this.maxInlineBlockSize, that.maxInlineBlockSize) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName()) &&
                Objects.equal(this.getQueryId(), that.getQueryId());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, schema, split, constraints, maxBlockSize, maxInlineBlockSize,
                getRequestType(), getCatalogName(), getQueryId());
    }
}

