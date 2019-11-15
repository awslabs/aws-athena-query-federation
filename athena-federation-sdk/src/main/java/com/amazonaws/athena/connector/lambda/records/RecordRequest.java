package com.amazonaws.athena.connector.lambda.records;

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

import static java.util.Objects.requireNonNull;

/**
 * Parent class representing the generic input of all <code>Record</code> operations.
 */
public abstract class RecordRequest
        extends FederationRequest
{
    private final RecordRequestType requestType;
    private final String catalogName;
    private final String queryId;

    /**
     * Constructs a new RecordRequest object.
     *
     * @param identity The identity of the caller.
     * @param requestType The type of the request.
     * @param catalogName The catalog name that data is requested for.
     * @param queryId The ID of the query requesting data.
     */
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

    /**
     * Returns the type of the request.
     *
     * @return The type of the request.
     */
    public RecordRequestType getRequestType()
    {
        return requestType;
    }

    /**
     * Returns the catalog name that data is requested for.
     *
     * @return The catalog name that data is requested for.
     */
    public String getCatalogName()
    {
        return catalogName;
    }

    /**
     * Returns the ID of the query requesting data.
     *
     * @return The ID of the query requesting data.
     */
    public String getQueryId()
    {
        return queryId;
    }
}
