/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PaginatedRequestIterator<T> implements Iterator<T>
{
    final Function<String, T> fetchPage;
    final Function<T, String> getToken;

    boolean notStarted = true;
    String nextPageToken = null;

    // This is a static to avoid the possibility of users retaining a reference to the iterator
    // and trying to use it after converting it to a Stream.
    public static <X> Stream<X> stream(Function<String, X> fetchPage, Function<X, String> getToken)
    {
        return StreamSupport.stream(
             Spliterators.spliteratorUnknownSize(
                new PaginatedRequestIterator<X>(fetchPage, getToken),
                Spliterator.IMMUTABLE | Spliterator.ORDERED),
            false);
    }

    public PaginatedRequestIterator(Function<String, T> fetchPage, Function<T, String> getToken)
    {
        this.fetchPage = fetchPage;
        this.getToken = getToken;
    }

    @Override
    public boolean hasNext()
    {
        return notStarted || nextPageToken != null;
    }

    @Override
    public T next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException("No more pages left");
        }

        T current = fetchPage.apply(nextPageToken);
        notStarted = false;
        nextPageToken = getToken.apply(current);
        return current;
    }
}
