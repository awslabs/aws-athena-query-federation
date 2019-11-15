package com.amazonaws.athena.connector.lambda.metadata;

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

import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents the input of a <code>GetSplits</code> operation.
 */
public class GetSplitsRequest
        extends MetadataRequest
{
    private final TableName tableName;
    private final Block partitions;
    private final List<String> partitionCols;
    private final Constraints constraints;
    private final String continuationToken;

    /**
     * Constructs a new GetSplitsRequest object.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that splits should be generated for.
     * @param tableName The table name that splits should be generated for.
     * @param partitions The partitions that splits should be generated for.
     * @param partitionCols The partition columns used for partitioning.
     * @param constraints The constraints that can be applied to split generation.
     * @param continuationToken The continuation token from which split generation should be resumed.
     */
    @JsonCreator
    public GetSplitsRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("partitions") Block partitions,
            @JsonProperty("partitionCols") List<String> partitionCols,
            @JsonProperty("constraints") Constraints constraints,
            @JsonProperty("continuationToken") String continuationToken)
    {
        super(identity, MetadataRequestType.GET_SPLITS, queryId, catalogName);
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");
        requireNonNull(partitionCols, "partitionCols is null");
        requireNonNull(constraints, "constraints is null");
        this.tableName = tableName;
        this.partitions = partitions;
        this.partitionCols = Collections.unmodifiableList(partitionCols);
        this.constraints = constraints;
        this.continuationToken = continuationToken;
    }

    /**
     * Constructs a new GetSplitsRequest object from an existing one with the ability to change the continuation token.
     * Helpful when making a continuation call since it requires the original request but updated token.
     *
     * @param clone The original request.
     * @param continuationToken The continuation token for the next paginated request.
     */
    public GetSplitsRequest(GetSplitsRequest clone, String continuationToken)
    {
        this(clone.getIdentity(), clone.getQueryId(), clone.getCatalogName(), clone.tableName, clone.partitions, clone.partitionCols, clone.constraints, continuationToken);
    }

    /**
     * Returns the continuation token for split generation.  If present, this is a paginated request and
     * split generation should be resumed using the provided token.
     *
     * @return The continuation token for the next paginated request.
     */
    @JsonProperty
    public String getContinuationToken()
    {
        return continuationToken;
    }

    /**
     * Returns the table name that splits should be generated for.
     *
     * @return The table name that splits should be generated for.
     */
    @JsonProperty
    public TableName getTableName()
    {
        return tableName;
    }

    /**
     * Convenience method to get the partition schema from partitions Block.
     *
     * @return The schema of the partitions.
     */
    @Transient
    public Schema getSchema()
    {
        return partitions.getSchema();
    }

    /**
     * Returns the partition columns used for partitioning.
     *
     * @return The partition columns used for partitioning.
     */
    @JsonProperty
    public List<String> getPartitionCols()
    {
        return partitionCols;
    }

    /**
     * Returns the partitions that splits should be generated for.
     *
     * @return The partitions that splits should be generated for.
     */
    @JsonProperty
    public Block getPartitions()
    {
        return partitions;
    }

    /**
     * Returns the constraints that can be applied to split generation.
     *
     * @return The constraints that can be applied to split generation.
     */
    @JsonProperty
    public Constraints getConstraints()
    {
        return constraints;
    }

    /**
     * Convenience method that returns whether a continuation token is provided in this request.  If true,
     * this is a paginated request and split generation should be resumed using the provided token.
     *
     * @return The continuation token from which split generation should be resumed.
     */
    @Transient
    public boolean hasContinuationToken()
    {
        return continuationToken != null;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("queryId", getQueryId())
                .add("tableName", tableName)
                .add("partitionCols", partitionCols)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .add("partitions", partitions)
                .add("constraints", constraints)
                .add("continuationToken", continuationToken)
                .toString();
    }

    /**
     * Frees up resources associated with the <code>partitions</code> and <code>constraints</code> Blocks.
     */
    @Override
    public void close()
            throws Exception
    {
        partitions.close();
        constraints.close();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetSplitsRequest that = (GetSplitsRequest) o;

        Objects.equal(this.tableName, that.tableName);
        Objects.equal(this.partitions, that.partitions);
        CollectionsUtils.equals(this.partitionCols, that.partitionCols);
        Objects.equal(this.continuationToken, that.continuationToken);
        Objects.equal(this.getRequestType(), that.getRequestType());
        Objects.equal(this.getCatalogName(), that.getCatalogName());

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.partitions, that.partitions) &&
                CollectionsUtils.equals(this.partitionCols, that.partitionCols) &&
                Objects.equal(this.continuationToken, that.continuationToken) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, partitions, partitionCols, continuationToken, getRequestType(), getCatalogName());
    }
}
