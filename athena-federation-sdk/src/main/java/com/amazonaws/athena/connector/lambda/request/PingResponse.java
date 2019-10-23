package com.amazonaws.athena.connector.lambda.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PingResponse
        extends FederationResponse
{
    private final int capabilities;
    private final String catalogName;
    private final String queryId;
    private final String sourceType;

    @JsonCreator
    public PingResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("sourceType") String sourceType,
            @JsonProperty("capabilities") int capabilities)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(queryId, "queryId is null");
        this.catalogName = catalogName;
        this.queryId = queryId;
        this.sourceType = sourceType;
        this.capabilities = capabilities;
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

    @JsonProperty("sourceType")
    public String getSourceType()
    {
        return sourceType;
    }

    @JsonProperty("capabilities")
    public int getCapabilities()
    {
        return capabilities;
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
                ", sourceType='" + sourceType + '\'' +
                ", capabilities='" + capabilities + '\'' +
                '}';
    }
}
