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

/**
 * Represents the input of a <code>ListTables</code> operation.
 */
public class ListTablesRequest
        extends MetadataRequest
{
    /**
     * Value used to indicate that the page size is unlimited, and therefore, the request should NOT be paginated.
     */
    public static final int UNLIMITED_PAGE_SIZE_VALUE = -1;

    private final String schemaName;
    private final String nextToken;
    private final int pageSize;

    /**
     * Constructs a new ListTablesRequest object.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that tables should be listed for.
     * @param schemaName The schema name that tables should be listed for. This may be null if no specific schema is
     *                   requested.
     * @param nextToken The pagination starting point for the next page (null indicates the first paginated request).
     * @param pageSize The page size used for pagination (UNLIMITED_PAGE_SIZE_VALUE indicates the request should not be
     *                 paginated).
     */
    @JsonCreator
    public ListTablesRequest(@JsonProperty("identity") FederatedIdentity identity,
                             @JsonProperty("queryId") String queryId,
                             @JsonProperty("catalogName") String catalogName,
                             @JsonProperty("schemaName") String schemaName,
                             @JsonProperty("nextToken") String nextToken,
                             @JsonProperty("pageSize") int pageSize)
    {
        super(identity, MetadataRequestType.LIST_TABLES, queryId, catalogName);
        this.schemaName = schemaName;
        this.nextToken = nextToken;
        this.pageSize = pageSize;
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
     * Gets the pagination starting point for the next page.
     * @return The pagination starting point for the next page (null indicates the first paginated request).
     */
    public String getNextToken()
    {
        return nextToken;
    }

    /**
     * Gets the page size used for pagination.
     * @return The page size used for pagination (UNLIMITED_PAGE_SIZE_VALUE indicates the request should not be paginated).
     */
    public int getPageSize()
    {
        return pageSize;
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
                ", nextToken='" + nextToken + '\'' +
                ", pageSize=" + pageSize +
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
                Objects.equal(this.getNextToken(), that.getNextToken()) &&
                Objects.equal(this.getPageSize(), that.getPageSize());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, getRequestType(), getCatalogName(), getNextToken(), getPageSize());
    }
}
