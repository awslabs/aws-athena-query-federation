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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.function.Function;

/**
 * A paginator for a Collection of type T (where T is sortable by converting it to String). The paginator will return
 * paginated results based on a page size specified in the constructor.
 * @param <T> The type of the item to be paginated.
 */
public class Paginator<T>
{
    private final PriorityQueue<Pair<String, T>> nameQueue;
    private final int pageSize;

    /**
     * Constructor for the paginator.
     * @param items A collection to T items to be paginated.
     * @param startName A name used as the start of the paginated results.
     * @param pageSize The maximum number of T items that can be be returned in each paginated result.
     * @param getNameFromItem A function that will convert a T item to String.
     */
    public Paginator(Collection<T> items, String startName, int pageSize, Function<T, String> getNameFromItem)
    {
        this.nameQueue = new PriorityQueue<>(Entry.comparingByKey());
        this.pageSize = pageSize;

        items.forEach(item -> {
            String name = getNameFromItem.apply(item);
            if (startName == null || name.compareTo(startName) >= 0) {
                // If startName exists, only strings that are equal or are higher in value than it will be added.
                nameQueue.add(Pair.of(name, item));
            }
        });
    }

    /**
     * Gets the next result page based on the page-size.
     * @return A paginated response containing a paginated Collection of T items, and the starting name for the next
     * paginated results. If there are no more items in the queue to paginate over, or the queue contains fewer items
     * than the indicated page size, the result may contain an empty or semi-populated Collection, and an empty String
     * for the next starting name.
     */
    public PaginatedResponse<T> getNextPage()
    {
        List<T> results = new ArrayList<>();

        for (int i = 0; i < pageSize && !nameQueue.isEmpty(); ++i) {
            results.add(nameQueue.poll().getValue());
        }

        return new PaginatedResponse<>(results, peekNextName());
    }

    /**
     * Peeks at the next name in the queue.
     * @return The next name in the queue, or an empty string if the queue is empty.
     */
    private String peekNextName()
    {
        if (nameQueue.isEmpty()) {
            return "";
        }

        return nameQueue.peek().getKey();
    }
}
