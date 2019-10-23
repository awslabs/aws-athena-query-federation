package com.amazonaws.athena.connectors.android;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryRequest {
    private final String queryId;
    private final String query;
    private final String echoValue;
    private final String responseQueue;

    @JsonCreator
    public QueryRequest(@JsonProperty("queryId") String queryId,
            @JsonProperty("query") String query,
            @JsonProperty("echoValue") String echoValue,
            @JsonProperty("responseQueue") String responseQueue) {
        this.queryId = queryId;
        this.query = query;
        this.echoValue = echoValue;
        this.responseQueue = responseQueue;
    }

    private QueryRequest(Builder builder) {
        queryId = builder.queryId;
        query = builder.query;
        echoValue = builder.echoValue;
        responseQueue = builder.responseQueue;
    }

    @JsonProperty("query")
    public String getQuery() {
        return query;
    }

    @JsonProperty("echoValue")
    public String getEchoValue() {
        return echoValue;
    }

    @JsonProperty("queryId")
    public String getQueryId() {
        return queryId;
    }

    @JsonProperty("responseQueue")
    public String getResponseQueue() {
        return responseQueue;
    }

    @Override
    public String toString() {
        return "QueryRequest{" +
                "queryId='" + queryId + '\'' +
                ", query='" + query + '\'' +
                ", echoValue='" + echoValue + '\'' +
                ", responseQueue='" + responseQueue + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String queryId;
        private String query;
        private String echoValue;
        private String responseQueue;

        private Builder() {
        }

        public Builder withQuery(String val) {
            query = val;
            return this;
        }

        public Builder withEchoValue(String val) {
            echoValue = val;
            return this;
        }

        public Builder withResponseQueue(String val) {
            responseQueue = val;
            return this;
        }

        public Builder withQueryId(String val) {
            queryId = val;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(this);
        }
    }
}
