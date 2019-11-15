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

import java.util.List;

public class QueryResponse
{
    private final String deviceId;
    private final String queryId;
    private final String name;
    private final String echoValue;
    private final List<String> values;
    private final int random;

    @JsonCreator
    public QueryResponse(@JsonProperty("deviceId") String deviceId,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("name") String name,
            @JsonProperty("echoValue") String echoValue,
            @JsonProperty("values") List<String> values,
            @JsonProperty("random") int random)
    {
        this.deviceId = deviceId;
        this.queryId = queryId;
        this.name = name;
        this.echoValue = echoValue;
        this.values = values;
        this.random = random;
    }

    private QueryResponse(Builder builder)
    {
        queryId = builder.queryId;
        deviceId = builder.deviceId;
        name = builder.name;
        echoValue = builder.echoValue;
        values = builder.values;
        random = builder.random;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    @JsonProperty("deviceId")
    public String getDeviceId()
    {
        return deviceId;
    }

    @JsonProperty("queryId")
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("echoValue")
    public String getEchoValue()
    {
        return echoValue;
    }

    @JsonProperty("values")
    public List<String> getValues()
    {
        return values;
    }

    @JsonProperty("random")
    public int getRandom()
    {
        return random;
    }

    @Override
    public String toString()
    {
        return "QueryResponse{" +
                "deviceId='" + deviceId + '\'' +
                ", queryId='" + queryId + '\'' +
                ", name='" + name + '\'' +
                ", echoValue='" + echoValue + '\'' +
                ", values=" + values +
                ", random=" + random +
                '}';
    }

    public static final class Builder
    {
        private String deviceId;
        private String queryId;
        private String name;
        private String echoValue;
        private List<String> values;
        private int random;

        private Builder()
        {
        }

        public Builder withDeviceId(String val)
        {
            deviceId = val;
            return this;
        }

        public Builder withQueryId(String val)
        {
            queryId = val;
            return this;
        }

        public Builder withEchoValue(String val)
        {
            echoValue = val;
            return this;
        }

        public Builder withName(String val)
        {
            name = val;
            return this;
        }

        public Builder withValues(List<String> val)
        {
            values = val;
            return this;
        }

        public Builder withRandom(int val)
        {
            random = val;
            return this;
        }

        public QueryResponse build()
        {
            return new QueryResponse(this);
        }
    }
}
