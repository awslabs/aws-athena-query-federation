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
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents the output of a <code>ListTables</code> operation.
 */
public class ListTablesResponse
        extends MetadataResponse
{
    private final Collection<TableName> tables;
    private final Optional<String> nextToken;

    /**
     * Constructs a new ListTablesResponse object.
     *
     * @param catalogName The catalog name that tables were listed for.
     * @param tables The list of table names (they all must be lowercase).
     */
    @JsonCreator
    public ListTablesResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("tables") Collection<TableName> tables)
    {
        this(catalogName, tables, null);
    }

    /**
     * Constructs a new ListTablesResponse object that could contain a paginated list of tables. A non-null nextToken
     * indicates that the response from the Lambda was paginated based on the nextToken value (passed in from the
     * request) in conjunction with the page-size environment variable (list_tables_page_size) passed in from the Lambda.
     *
     * @param catalogName The catalog name that tables were listed for.
     * @param tables The list of table names (they all must be lowercase).
     * @param nextToken The pagination starting point for the next page (i.e. the starting table for the next request).
     */
    @JsonCreator
    public ListTablesResponse(@JsonProperty("catalogName") String catalogName,
                              @JsonProperty("tables") Collection<TableName> tables,
                              @JsonProperty("nextToken") String nextToken)
    {
        super(MetadataRequestType.LIST_TABLES, catalogName);
        requireNonNull(tables, "tables is null");
        this.tables = Collections.unmodifiableCollection(tables);
        this.nextToken = Optional.ofNullable(nextToken);
    }

    /**
     * Returns the list of table names.
     *
     * @return The list of table names.
     */
    public Collection<TableName> getTables()
    {
        return tables;
    }

    /**
     * Returns the nextToken (the starting table for the next request).
     * @return Optional String representing the starting table for the next request (if the Optional is present). If
     * The Optional is empty, the response was not paginated.
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
        return "ListTablesResponse{" +
                "tables=" + tables +
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

        ListTablesResponse that = (ListTablesResponse) o;

        return CollectionsUtils.equals(this.tables, that.tables) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName()) &&
                Objects.equal(this.getNextToken(), that.getNextToken());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tables, getRequestType(), getCatalogName(), getNextToken());
    }
}
