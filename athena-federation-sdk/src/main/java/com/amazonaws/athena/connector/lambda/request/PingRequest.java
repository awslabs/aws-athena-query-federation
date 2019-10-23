package com.amazonaws.athena.connector.lambda.request;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PingRequest
        extends FederationRequest
{
    private final String catalogName;
    private final String queryId;

    @JsonCreator
    public PingRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("queryId") String queryId)
    {
        super(identity);
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(queryId, "queryId is null");
        this.catalogName = catalogName;
        this.queryId = queryId;
    }

    @JsonProperty("catalogName")
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty("queryId")
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public void close()
            throws Exception
    {
        //no-op
    }

    @Override
    public String toString()
    {
        return "PingRequest{" +
                "catalogName='" + catalogName + '\'' +
                ", queryId='" + queryId + '\'' +
                '}';
    }
}
