package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import static java.util.Objects.requireNonNull;

public abstract class MetadataRequest
       extends FederationRequest
{
    private final MetadataRequestType requestType;
    private final String queryId;
    private final String catalogName;

    public MetadataRequest(FederatedIdentity identity, MetadataRequestType requestType, String queryId, String catalogName)
    {
        super(identity);
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
        this.queryId = queryId;
    }

    public MetadataRequestType getRequestType()
    {
        return requestType;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getQueryId()
    {
        return queryId;
    }
}
