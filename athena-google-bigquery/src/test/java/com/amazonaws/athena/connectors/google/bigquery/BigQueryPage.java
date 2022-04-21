/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.google.api.gax.paging.Page;

import java.util.Collection;
import java.util.Iterator;

/**
 * This class is a wrapper around the {@link Page} class as a convient way to create Pages in unit tests.
 *
 * @param <T> The type of object that is being returned from a Google BigQuery API call. For example, getDatasets().
 */
class BigQueryPage<T>
        implements Page<T>
{
    final Collection<T> collection;

    BigQueryPage(Collection<T> collection)
    {
        this.collection = collection;
    }

    @Override
    public boolean hasNextPage()
    {
        return false;
    }

    @Override
    public String getNextPageToken()
    {
        return null;
    }

    @Override
    public Page<T> getNextPage()
    {
        return null;
    }

    @Override
    public Iterable<T> iterateAll()
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return collection.iterator();
            }
        };
    }

    @Override
    public Iterable<T> getValues()
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return collection.iterator();
            }
        };
    }
}
