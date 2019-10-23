package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static java.util.Objects.requireNonNull;

public class GetTableRequest
        extends MetadataRequest
{
    private final TableName tableName;

    @JsonCreator
    public GetTableRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName)
    {
        super(identity, MetadataRequestType.GET_TABLE, queryId, catalogName);
        requireNonNull(tableName, "tableName is null");
        this.tableName = tableName;
    }

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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        GetTableRequest that = (GetTableRequest) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, getRequestType(), getCatalogName());
    }
}
