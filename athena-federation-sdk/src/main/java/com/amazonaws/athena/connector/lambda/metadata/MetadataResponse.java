package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.request.FederationResponse;

import static java.util.Objects.requireNonNull;

public abstract class MetadataResponse
        extends FederationResponse
{
    private final MetadataRequestType requestType;
    private final String catalogName;

    public MetadataResponse(MetadataRequestType requestType, String catalogName)
    {
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
    }

    public MetadataRequestType getRequestType()
    {
        return requestType;
    }

    public String getCatalogName()
    {
        return catalogName;
    }
}
