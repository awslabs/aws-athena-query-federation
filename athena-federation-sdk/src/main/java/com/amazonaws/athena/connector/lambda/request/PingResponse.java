package com.amazonaws.athena.connector.lambda.request;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
