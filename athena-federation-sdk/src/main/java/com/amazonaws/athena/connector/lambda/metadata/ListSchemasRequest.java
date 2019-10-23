package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class ListSchemasRequest
        extends MetadataRequest
{
    @JsonCreator
    public ListSchemasRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName)
    {
        super(identity, MetadataRequestType.LIST_SCHEMAS, queryId, catalogName);
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

        ListSchemasRequest that = (ListSchemasRequest) o;

        return Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getRequestType(), getCatalogName());
    }
}
