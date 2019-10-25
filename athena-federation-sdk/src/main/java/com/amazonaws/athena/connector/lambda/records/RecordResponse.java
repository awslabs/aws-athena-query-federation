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

import com.amazonaws.athena.connector.lambda.request.FederationResponse;

import static java.util.Objects.requireNonNull;

public abstract class RecordResponse
        extends FederationResponse
{
    private final RecordRequestType requestType;
    private final String catalogName;

    public RecordResponse(RecordRequestType requestType, String catalogName)
    {
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
    }

    public RecordRequestType getRequestType()
    {
        return requestType;
    }

    public String getCatalogName()
    {
        return catalogName;
    }
}

