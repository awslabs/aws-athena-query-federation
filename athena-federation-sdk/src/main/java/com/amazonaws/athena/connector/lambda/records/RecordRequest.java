package com.amazonaws.athena.connector.lambda.records;

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import static java.util.Objects.requireNonNull;

public abstract class RecordRequest
        extends FederationRequest
{
    private final RecordRequestType requestType;
    private final String catalogName;
    private final String queryId;

    public RecordRequest(FederatedIdentity identity, RecordRequestType requestType, String catalogName, String queryId)
    {
        super(identity);
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(queryId, "queryId is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
        this.queryId = queryId;
    }

    public RecordRequestType getRequestType()
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
