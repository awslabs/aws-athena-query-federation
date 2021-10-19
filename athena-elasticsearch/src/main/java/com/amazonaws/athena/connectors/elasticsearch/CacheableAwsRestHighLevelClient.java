/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import org.apache.arrow.util.VisibleForTesting;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class CacheableAwsRestHighLevelClient
{
    /**
     * The maximum age allowed for stored clients in milliseconds (15 minutes).
     */
    protected static final long MAX_CACHE_AGE_MS = 900_000;
    /**
     * The maximum number of clients allowed in the cache.
     */
    protected static final int MAX_CACHE_SIZE = 10;
    /**
     * Stored cache of clients accessible via the endpoint. This cache has a maximum capacity of 10 clients.
     * The mechanism for removing clients from the cache is outlined in the following eviction policy:
     * 1) An age test is performed when a client is retrieved from cache, and upon insertion into cache. Clients that
     *    have aged out (as indicated by MAX_CACHE_AGE_MS), will be removed.
     * 2) A health test is performed on a client retrieved from cache. A client that fails the test, will be removed.
     * 3) A cache capacity test is performed upon insertion into cache. The oldest client will be removed when the
     *    cache size exceeds capacity (as indicated by MAX_CACHE_SIZE).
     */
    private final Map<String, CacheEntry> clientCache = new LinkedHashMap<>();

    public CacheableAwsRestHighLevelClient() {}

    /**
     * Gets a client from the cache keyed on the endpoint. If the client retrieved has aged out, or is unhealthy,
     * then evict it.
     * @param endpoint is the http address associated with the client.
     * @return a client (if one exist, is age-appropriate, and is healthy), or null (if none of the aforementioned
     * conditions are met).
     */
    public AwsRestHighLevelClient get(String endpoint)
    {
        CacheEntry cacheEntry = clientCache.get(endpoint);

        if (cacheEntry != null) {
            if (cacheEntry.getAge() > MAX_CACHE_AGE_MS) {
                // Gentle eviction of cache based on client age only.
                evictCache(false);
                return null;
            }
            if (!clientIsHealthy(cacheEntry.getClient())) {
                // Forceful eviction of unhealthy client.
                evictCache(endpoint);
                return null;
            }
        }

        return cacheEntry == null ? null : cacheEntry.getClient();
    }

    /**
     * Sets a client in the cache keyed on the endpoint and process cache eviction based on cache capacity limit.
     * @param endpoint is the http address associated with the client.
     * @param client is a REST client used for communication with an Elasticsearch instance.
     */
    public void put(String endpoint, AwsRestHighLevelClient client)
    {
        clientCache.put(endpoint, new CacheEntry(client));
        // Forceful eviction of cache based on client age and cache capacity limit.
        evictCache(size() > MAX_CACHE_SIZE);
    }

    /**
     * Evicts clients that have aged out, or exceeded the cache's capacity limit as indicated by the force argument.
     * Evicted clients will be closed in order to free up their resources.
     * @param force is a flag that indicated whether eviction will be handled forcefully (force = true), or more
     *              gently (force = false). The latter will only evict based on the age of the client, while the
     *              former will forcefully evict the oldest client if the cache's capacity has been exceeded.
     */
    private void evictCache(boolean force)
    {
        Iterator<Map.Entry<String, CacheableAwsRestHighLevelClient.CacheEntry>> itr = clientCache.entrySet().iterator();
        int removed = 0;
        while (itr.hasNext()) {
            CacheableAwsRestHighLevelClient.CacheEntry entry = itr.next().getValue();
            // If age of client is greater than the maximum allowed, remove it.
            if (entry.getAge() > MAX_CACHE_AGE_MS) {
                closeClient(entry.getClient());
                itr.remove();
                removed++;
                continue;
            }
            // Since a LinkedHashMap is ordered from older to newer, no need to keep looking once we encounter
            // an age-appropriate client.
            break;
        }

        if (removed == 0 && force) {
            // Remove the oldest entry since we found no expired entries and cache has exceeded capacity.
            itr = clientCache.entrySet().iterator();
            if (itr.hasNext()) {
                closeClient(itr.next().getValue().getClient());
                itr.remove();
            }
        }
    }

    /**
     * Forcefully evict a client whose endpoint is specified from the cache. Evicted clients will be closed in order
     * to free up their resources.
     * @param endpoint of the client to be evicted.
     */
    private void evictCache(String endpoint)
    {
        CacheEntry cacheEntry = clientCache.remove(endpoint);
        closeClient(cacheEntry.getClient());
    }

    /**
     * Test the client's connection by pinging the Elasticsearch cluster.
     * @param client is the REST client to be tested.
     * @return true if the client's connection is healthy, false otherwise.
     */
    private boolean clientIsHealthy(AwsRestHighLevelClient client)
    {
        try {
            return client.ping(RequestOptions.DEFAULT);
        }
        catch (IOException error) {
            return false;
        }
    }

    /**
     * Frees up all resources held by client after its eviction from the cache.
     * @param client is the REST client who's just been evicted.
     */
    private void closeClient(AwsRestHighLevelClient client)
    {
        try {
            client.close();
        }
        catch (IOException error) {
            // Do nothing.
        }
    }

    /**
     * @return the number of clients stored in the cache.
     */
    public int size()
    {
        return clientCache.size();
    }

    @VisibleForTesting
    protected void addClientEntry(String endpoint, AwsRestHighLevelClient client, long createTime)
    {
        clientCache.put(endpoint, new CacheEntry(client, createTime));
    }

    /**
     * Clients are stored in the cache using a CacheEntry instance which consists of the client as well as the time
     * of creation. The latter is used for the age test during the eviction from the cache process.
     */
    private class CacheEntry
    {
        private final AwsRestHighLevelClient client;
        private final long createTime;

        public CacheEntry(AwsRestHighLevelClient client)
        {
            this(client, System.currentTimeMillis());
        }

        public CacheEntry(AwsRestHighLevelClient client, long createTime)
        {
            this.client = client;
            this.createTime = createTime;
        }

        /**
         * Get the client from the cache entry.
         * @return the REST client.
         */
        public AwsRestHighLevelClient getClient()
        {
            return client;
        }

        /**
         * Gets the age of the client entry in milliseconds.
         * @return the calculated age (in millisecond) based on the difference between the current time and createdTime.
         */
        public long getAge()
        {
            return System.currentTimeMillis() - createTime;
        }
    }
}
