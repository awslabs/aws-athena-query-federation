/*-
 * #%L
 * athena-android
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.android;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryRequest
{
    private final String queryId;
    private final String query;
    private final String echoValue;
    private final String responseQueue;

    @JsonCreator
    public QueryRequest(@JsonProperty("queryId") String queryId,
            @JsonProperty("query") String query,
            @JsonProperty("echoValue") String echoValue,
            @JsonProperty("responseQueue") String responseQueue)
    {
        this.queryId = queryId;
        this.query = query;
        this.echoValue = echoValue;
        this.responseQueue = responseQueue;
    }

    private QueryRequest(Builder builder)
    {
        queryId = builder.queryId;
        query = builder.query;
        echoValue = builder.echoValue;
        responseQueue = builder.responseQueue;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @JsonProperty("query")
    public String getQuery()
    {
        return query;
    }

    @JsonProperty("echoValue")
    public String getEchoValue()
    {
        return echoValue;
    }

    @JsonProperty("queryId")
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty("responseQueue")
    public String getResponseQueue()
    {
        return responseQueue;
    }

    @Override
    public String toString()
    {
        return "QueryRequest{" +
                "queryId='" + queryId + '\'' +
                ", query='" + query + '\'' +
                ", echoValue='" + echoValue + '\'' +
                ", responseQueue='" + responseQueue + '\'' +
                '}';
    }

    public static final class Builder
    {
        private String queryId;
        private String query;
        private String echoValue;
        private String responseQueue;

        private Builder()
        {
        }

        public Builder withQuery(String val)
        {
            query = val;
            return this;
        }

        public Builder withEchoValue(String val)
        {
            echoValue = val;
            return this;
        }

        public Builder withResponseQueue(String val)
        {
            responseQueue = val;
            return this;
        }

        public Builder withQueryId(String val)
        {
            queryId = val;
            return this;
        }

        public QueryRequest build()
        {
            return new QueryRequest(this);
        }
    }
}
