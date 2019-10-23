package com.amazonaws.athena.connector.lambda.request;

import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ListSchemasResponse.class, name = "ListSchemasResponse"),
        @JsonSubTypes.Type(value = ListTablesResponse.class, name = "ListTablesResponse"),
        @JsonSubTypes.Type(value = GetTableResponse.class, name = "GetTableResponse"),
        @JsonSubTypes.Type(value = GetTableLayoutResponse.class, name = "GetTableLayoutResponse"),
        @JsonSubTypes.Type(value = GetSplitsResponse.class, name = "GetSplitsResponse"),
        @JsonSubTypes.Type(value = ReadRecordsResponse.class, name = "ReadRecordsResponse"),
        @JsonSubTypes.Type(value = RemoteReadRecordsResponse.class, name = "RemoteReadRecordsResponse"),
        @JsonSubTypes.Type(value = PingResponse.class, name = "PingResponse")
})
public abstract class FederationResponse implements AutoCloseable
{
}
