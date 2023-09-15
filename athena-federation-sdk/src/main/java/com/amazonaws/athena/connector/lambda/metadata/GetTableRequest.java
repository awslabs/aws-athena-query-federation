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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Represents the input of a <code>GetTable</code> operation.
 */
@JsonIgnoreProperties(value = {"queryPassthroughArguments"}, allowGetters = true, allowSetters = true)
public class GetTableRequest
        extends MetadataRequest
{
    private final TableName tableName;
    private Map<String, String> queryPassthroughArguments;

    /**
     * Constructs a new GetTableRequest object.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that the table belongs to.
     * @param tableName The name of the table metadata is being requested for.
     */
    @JsonCreator
    public GetTableRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName)
    {
        super(identity, MetadataRequestType.GET_TABLE, queryId, catalogName);
        requireNonNull(tableName, "tableName is null");
        this.tableName = tableName;
        this.queryPassthroughArguments = Collections.emptyMap();
    }

    /**
     * Returns the name of the table metadata is being requested for.
     *
     * @return The name of the table metadata is being requested for.
     */
    public TableName getTableName()
    {
        return tableName;
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
    }

    @JsonGetter("queryPassthroughArguments")
    public Map<String, String> getQueryPassthroughArguments()
    {
        return this.queryPassthroughArguments;
    }

    @JsonSetter("queryPassthroughArguments")
    public void setQueryPassthroughArguments(Map<String, String> queryPassthroughArguments)
    {
        this.queryPassthroughArguments = requireNonNull(queryPassthroughArguments, "queryPassthroughArguments is null");
    }
    @Override
    public String toString()
    {
        return "GetTableRequest{" +
                "queryId=" + getQueryId() +
                ", tableName=" + tableName +
                "queryPassthroughArguments=" + queryPassthroughArguments +
                '}';
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

        GetTableRequest that = (GetTableRequest) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName()) &&
                Objects.equal(this.getQueryPassthroughArguments(), that.getQueryPassthroughArguments());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, getRequestType(), getCatalogName(), getQueryPassthroughArguments());
    }
}
