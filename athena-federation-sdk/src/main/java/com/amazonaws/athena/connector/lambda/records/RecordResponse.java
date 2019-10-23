package com.amazonaws.athena.connector.lambda.records;

import com.amazonaws.athena.connector.lambda.request.FederationResponse;

import static java.util.Objects.requireNonNull;

public abstract class RecordResponse
        extends FederationResponse
{
    private final RecordRequestType requestType;
    private final String catalogName;

    public RecordResponse(RecordRequestType requestType, String catalogName)
    {
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
    }

    public RecordRequestType getRequestType()
    {
        return requestType;
    }

    public String getCatalogName()
    {
        return catalogName;
    }
}

