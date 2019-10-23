package com.amazonaws.athena.connector.lambda.request;

import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ListSchemasRequest.class, name = "ListSchemasRequest"),
        @JsonSubTypes.Type(value = ListTablesRequest.class, name = "ListTablesRequest"),
        @JsonSubTypes.Type(value = GetTableRequest.class, name = "GetTableRequest"),
        @JsonSubTypes.Type(value = GetTableLayoutRequest.class, name = "GetTableLayoutRequest"),
        @JsonSubTypes.Type(value = GetSplitsRequest.class, name = "GetSplitsRequest"),
        @JsonSubTypes.Type(value = ReadRecordsRequest.class, name = "ReadRecordsRequest"),
        @JsonSubTypes.Type(value = PingRequest.class, name = "PingRequest")
})
public abstract class FederationRequest
        implements AutoCloseable
{
    private final FederatedIdentity identity;

    public FederationRequest()
    {
        identity = null;
    }

    public FederationRequest(FederatedIdentity identity)
    {
        this.identity = identity;
    }

    public FederatedIdentity getIdentity()
    {
        return identity;
    }
}
