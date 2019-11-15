package com.amazonaws.athena.connector.lambda.metadata;

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

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Represents the input of a <code>ListSchemas</code> operation.
 */
public class ListSchemasRequest
        extends MetadataRequest
{
    /**
     * Constructs a new ListSchemasRequest object.
     *
     * @param identity The identity of the caller.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that schemas should be listed for.
     */
    @JsonCreator
    public ListSchemasRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName)
    {
        super(identity, MetadataRequestType.LIST_SCHEMAS, queryId, catalogName);
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
    }

    @Override
    public String toString()
    {
        return "ListSchemasRequest{" + "queryId=" + getQueryId() + "}";
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

        ListSchemasRequest that = (ListSchemasRequest) o;

        return Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(getRequestType(), getCatalogName());
    }
}
