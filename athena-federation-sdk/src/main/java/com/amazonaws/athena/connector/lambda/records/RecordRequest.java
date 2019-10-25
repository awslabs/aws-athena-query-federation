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

public abstract class RecordRequest
        extends FederationRequest
{
    private final RecordRequestType requestType;
    private final String catalogName;
    private final String queryId;

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

    public RecordRequestType getRequestType()
    {
        return requestType;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getQueryId()
    {
        return queryId;
    }
}
