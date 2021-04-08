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

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Optional;

/**
 * Represents the input of a <code>ListTables</code> operation.
 */
public class ListTablesRequest
        extends MetadataRequest
{
    private final String schemaName;
    private final Optional<String> nextToken;

    /**
     * Constructs a new ListTablesRequest object.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that tables should be listed for.
     * @param schemaName The schema name that tables should be listed for. This may be null if no specific schema is requested.
     */
    @JsonCreator
    public ListTablesRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName)
    {
        this(identity, queryId, catalogName, schemaName, null);
    }

    /**
     * Constructs a new ListTablesRequest object that can be used to paginate the response from the Lambda based on
     * the value of nextToken. A non-null nextToken indicates that the response should be paginated based on the
     * nextToken value in conjunction with the page-size environment variable (list_tables_page_size) passed in from
     * the Lambda.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that tables should be listed for.
     * @param schemaName The schema name that tables should be listed for. This may be null if no specific schema is
     *                   requested.
     * @param nextToken The pagination starting point for the next page (i.e. the next table in the paginated response).
     */
    @JsonCreator
    public ListTablesRequest(@JsonProperty("identity") FederatedIdentity identity,
                             @JsonProperty("queryId") String queryId,
                             @JsonProperty("catalogName") String catalogName,
                             @JsonProperty("schemaName") String schemaName,
                             @JsonProperty("nextToken") String nextToken)
    {
        super(identity, MetadataRequestType.LIST_TABLES, queryId, catalogName);
        this.schemaName = schemaName;
        this.nextToken = Optional.ofNullable(nextToken);
    }

    /**
     * Returns the schema name that tables should be listed for. This may be null if no specific schema is requested.
     *
     * @return The schema name that tables should be listed for. This may be null if no specific schema is requested.
     */
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * Returns the nextToken (next table in the paginated response).
     * @return Optional String representing the next table in the paginated response (if the Optional is present). If
     * The Optional is empty, the response should not be paginated.
     */
    public Optional<String> getNextToken()
    {
        return nextToken;
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
    }

    @Override
    public String toString()
    {
        return "ListTablesRequest{" +
                "queryId=" + getQueryId() +
                ", schemaName='" + schemaName + '\'' +
                nextToken.map(table -> ", nextToken=" + table).orElse("") +
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

        ListTablesRequest that = (ListTablesRequest) o;

        return Objects.equal(this.schemaName, that.schemaName) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName()) &&
                Objects.equal(this.getNextToken(), that.getNextToken());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, getRequestType(), getCatalogName(), getNextToken());
    }
}
