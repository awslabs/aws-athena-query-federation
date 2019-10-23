package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class ListTablesRequest
        extends MetadataRequest
{
    private final String schemaName;

    /**
     * @param catalogName The name of the catalog being requested.
     * @param schemaName This may be null if no specific schema is requested.
     */
    @JsonCreator
    public ListTablesRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName)
    {
        super(identity, MetadataRequestType.LIST_TABLES, queryId, catalogName);
        this.schemaName = schemaName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        ListTablesRequest that = (ListTablesRequest) o;

        return Objects.equal(this.schemaName, that.schemaName) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, getRequestType(), getCatalogName());
    }
}
