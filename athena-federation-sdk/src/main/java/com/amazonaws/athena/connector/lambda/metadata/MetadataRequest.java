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

import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.services.lambda.runtime.Context;

import static java.util.Objects.requireNonNull;

/**
 * Parent class representing the generic input of all <code>Metadata</code> operations.
 */
public abstract class MetadataRequest
       extends FederationRequest
{
    private final MetadataRequestType requestType;
    private final String queryId;
    private final String catalogName;
    private Context context;

    /**
     * Constructs a new MetadataRequest object.
     *
     * @param identity The identity of the caller.
     * @param requestType The type of the request.
     * @param queryId The ID of the query requesting metadata.
     * @param catalogName The catalog name that metadata is requested for.
     */
    public MetadataRequest(FederatedIdentity identity, MetadataRequestType requestType, String queryId, String catalogName)
    {
        super(identity);
        this.requestType = requireNonNull(requestType, "requestType is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.queryId = queryId;
    }

    /**
     * Returns the type of the request.
     *
     * @return The type of the request.
     */
    public MetadataRequestType getRequestType()
    {
        return requestType;
    }

    /**
     * Returns the catalog name that metadata is requested for.
     *
     * @return The catalog name that metadata is requested for.
     */
    public String getCatalogName()
    {
        return catalogName;
    }

    /**
     * Returns the ID of the query requesting metadata.
     *
     * @return The ID of the query requesting metadata.
     */
    public String getQueryId()
    {
        return queryId;
    }

    /**
     * Returns Context from Lambda.
     *
     * @return The Context of the Lambda
     */
    public Context getContext()
    {
        return context;
    }

    /**
     * Set the Context from Lambda.
     */
    public void setContext(Context context)
    {
        this.context = context;
    }
}
