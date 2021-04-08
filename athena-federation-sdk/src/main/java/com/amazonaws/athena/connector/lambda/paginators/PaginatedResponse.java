/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.paginators;

import java.util.Collection;

/**
 * A paginated response from a paginator for a Collection of type T, containing a Collection of T-type items, and the
 * starting point for the next pagination.
 * @param <T> The type of the paginated items.
 */
public class PaginatedResponse<T>
{
    private final Collection<T> results;
    private final String nextName;

    /**
     * Constructor for the paginated response.
     * @param results A paginated Collection of T-type items.
     * @param nextName The starting point for the next pagination.
     */
    public PaginatedResponse(Collection<T> results, String nextName)
    {
        this.results = results;
        this.nextName = nextName;
    }

    /**
     * Accessor for the paginated results.
     * @return A paginated Collection of T-type items.
     */
    public Collection<T> getResults()
    {
        return results;
    }

    /**
     * Accessor for the next name.
     * @return The starting point for the next pagination.
     */
    public String getNextName()
    {
        return nextName;
    }

    @Override
    public String toString()
    {
        return "PaginatedResponse{" +
                "results=" + results +
                ", nextName='" + nextName + '\'' +
                '}';
    }
}
