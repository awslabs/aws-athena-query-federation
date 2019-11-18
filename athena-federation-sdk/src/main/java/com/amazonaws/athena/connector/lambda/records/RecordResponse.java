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

/**
 * Parent class representing the generic output of all <code>Record</code> operations.
 */
public abstract class RecordResponse
        extends FederationResponse
{
    private final RecordRequestType requestType;
    private final String catalogName;

    /**
     * Constructs a new RecordResponse object.
     *
     * @param requestType The type of request this response corresponds to.
     * @param catalogName The catalog name that the data is for.
     */
    public RecordResponse(RecordRequestType requestType, String catalogName)
    {
        requireNonNull(requestType, "requestType is null");
        requireNonNull(catalogName, "catalogName is null");
        this.requestType = requestType;
        this.catalogName = catalogName;
    }

    /**
     * Returns the type of request this response corresponds to.
     *
     * @return The type of request this response corresponds to.
     */
    public RecordRequestType getRequestType()
    {
        return requestType;
    }

    /**
     * Returns the catalog name that the data is for.
     *
     * @return The catalog name that the data is for.
     */
    public String getCatalogName()
    {
        return catalogName;
    }
}
