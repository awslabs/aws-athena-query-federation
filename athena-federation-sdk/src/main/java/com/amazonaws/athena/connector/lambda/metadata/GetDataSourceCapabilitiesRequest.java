/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class GetDataSourceCapabilitiesRequest
        extends MetadataRequest
{
    /**
     * Constructs a new MetadataRequest object.
     *
     * @param identity    The identity of the caller.
     * @param queryId     The ID of the query requesting metadata.
     * @param catalogName The catalog name that metadata is requested for.
     */
    @JsonCreator
    public GetDataSourceCapabilitiesRequest(@JsonProperty("identity") FederatedIdentity identity,
                                            @JsonProperty("queryId") String queryId,
                                            @JsonProperty("catalogName") String catalogName)
    {
        super(identity, MetadataRequestType.GET_DATASOURCE_CAPABILITIES, queryId, catalogName);
    }

    @Override
    public void close()
            throws Exception
    {
    }

    @Override
    public String toString()
    {
        return "GetDataSourceCapabilitiesRequest{" +
                "identity=" + getIdentity() +
                ", queryId=" + getQueryId() +
                ", catalogName=" + getCatalogName() +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetDataSourceCapabilitiesRequest that = (GetDataSourceCapabilitiesRequest) o;

        return Objects.equal(this.getIdentity(), that.getIdentity()) &&
                Objects.equal(this.getQueryId(), that.getQueryId()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName()) &&
                Objects.equal(this.getRequestType(), that.getRequestType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getIdentity(), getQueryId(), getCatalogName(), getRequestType());
    }
}
