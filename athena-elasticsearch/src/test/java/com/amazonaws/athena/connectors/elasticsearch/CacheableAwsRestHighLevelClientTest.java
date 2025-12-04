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

import org.elasticsearch.client.RequestOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the CacheableAwsRestHighLevelClient class.
 */
@RunWith(MockitoJUnitRunner.class)
public class CacheableAwsRestHighLevelClientTest
{
    private static final Logger logger = LoggerFactory.getLogger(CacheableAwsRestHighLevelClientTest.class);
    private static final String ENDPOINT_1 = "endpoint1";
    private static final String ENDPOINT_2 = "endpoint2";

    private CacheableAwsRestHighLevelClient clientCache;

    @Mock
    AwsRestHighLevelClient client1;

    @Mock
    AwsRestHighLevelClient client2;

    @Before
    public void setup()
    {
        clientCache = new CacheableAwsRestHighLevelClient();
    }

    /**
     * Test the cache's ability to return the correct client.
     */
    @Test
    public void get_withMultipleEndpoints_returnsCachedClientPerEndpoint()
            throws IOException
    {
        logger.info("get_withMultipleEndpoints_returnsCachedClientPerEndpoint - enter");

        clientCache.put(ENDPOINT_1, client1);
        clientCache.put(ENDPOINT_2, client2);

        when(client1.ping(RequestOptions.DEFAULT)).thenReturn(true);
        when(client2.ping(RequestOptions.DEFAULT)).thenReturn(true);

        assertEquals("Cache has wrong number of clients.", 2, clientCache.size());
        assertNotNull("Cache get() method returned null.", clientCache.get(ENDPOINT_1));
        assertNotNull("Cache get() method returned null.", clientCache.get(ENDPOINT_2));
        assertEquals("Wrong client returned from cache.", client1, clientCache.get(ENDPOINT_1));
        assertEquals("Wrong client returned from cache.", client2, clientCache.get(ENDPOINT_2));

        logger.info("get_withMultipleEndpoints_returnsCachedClientPerEndpoint - exit");
    }

    /**
     * Test the cache's ability to evict clients that have aged out (as indicated by MAX_CACHE_AGE_MS).
     */
    @Test
    public void get_whenClientExceedsMaxCacheAge_evictsClient()
            throws IOException
    {
        logger.info("get_whenClientExceedsMaxCacheAge_evictsClient - enter");

        lenient().when(client1.ping(any())).thenReturn(true);
        // Test eviction on the get() method.
        clientCache.addClientEntry(ENDPOINT_1, client1, System.currentTimeMillis() -
                CacheableAwsRestHighLevelClient.MAX_CACHE_AGE_MS - 1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get(ENDPOINT_1));
        assertEquals("Cache has wrong number of clients.", 0, clientCache.size());

        // Test eviction on the put() method.
        lenient().when(client2.ping(any())).thenReturn(true);
        clientCache.addClientEntry(ENDPOINT_1, client1, System.currentTimeMillis() -
                CacheableAwsRestHighLevelClient.MAX_CACHE_AGE_MS - 1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());

        clientCache.put(ENDPOINT_2, client2);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNotNull("Cache get() method returned null.", clientCache.get(ENDPOINT_2));
        assertEquals("Wrong client returned from cache.", client2, clientCache.get(ENDPOINT_2));

        logger.info("get_whenClientExceedsMaxCacheAge_evictsClient - exit");
    }

    /**
     * Test the cache's ability to evict unhealthy clients.
     */
    @Test
    public void get_whenClientPingFails_evictsUnhealthyClient()
            throws IOException
    {
        logger.info("get_whenClientPingFails_evictsUnhealthyClient - enter");

        when(client1.ping(any())).thenReturn(false);
        clientCache.put(ENDPOINT_1, client1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get(ENDPOINT_1));
        assertEquals("Cache has wrong number of clients.", 0, clientCache.size());

        logger.info("get_whenClientPingFails_evictsUnhealthyClient - exit");
    }

    /**
     * Test the cache's ability to evict the oldest clients when the cache's capacity is exceeded (as indicated by
     * MAX_CACHE_SIZE).
     */
    @Test
    public void put_whenCapacityExceeded_evictsOldestClient()
            throws IOException
    {
        logger.info("put_whenCapacityExceeded_evictsOldestClient - enter");

        lenient().when(client1.ping(any())).thenReturn(true);
        clientCache.put(ENDPOINT_1, client1);
        for (int i = 0; i < CacheableAwsRestHighLevelClient.MAX_CACHE_SIZE; ++i) {
            String endpoint = "newendpoint" + i;
            clientCache.put(endpoint, client2);
        }

        assertEquals("Cache has wrong number of clients.",
                CacheableAwsRestHighLevelClient.MAX_CACHE_SIZE, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get(ENDPOINT_1));

        logger.info("put_whenCapacityExceeded_evictsOldestClient - exit");
    }

    @Test
    public void get_withUnhealthyClient_evictsClientAndClosesIt()
            throws IOException
    {
        clientCache.put(ENDPOINT_1, client1);
        clientCache.put(ENDPOINT_2, client2);

        assertEquals("Cache should have 2 clients before get", 2, clientCache.size());

        // Make client1 unhealthy - get() will call evictCache(endpoint) internally
        when(client1.ping(any())).thenThrow(new IOException("Connection failed"));
        when(client2.ping(any())).thenReturn(true);

        // get() will detect unhealthy client and evict it
        assertNull("get() should return null for unhealthy client", clientCache.get(ENDPOINT_1));

        assertEquals("Cache should have 1 client after eviction", 1, clientCache.size());

        // Verify that client1 was closed during eviction
        verify(client1, times(1)).close();

        // Verify that client2 is still accessible after selective eviction
        assertNotNull("Healthy client should still be accessible", clientCache.get(ENDPOINT_2));
        assertEquals("get() should return client2 for endpoint2", client2, clientCache.get(ENDPOINT_2));
    }

    @Test
    public void get_whenUnhealthyClientCloseThrowsIOException_evictsClientWithoutRethrowing()
            throws IOException
    {
        doThrow(new IOException("Close failed")).when(client1).close();
        clientCache.put(ENDPOINT_1, client1);

        // Make client unhealthy - get() will call evictCache(endpoint) internally, which calls closeClient
        when(client1.ping(any())).thenThrow(new IOException("Connection failed"));

        // get() will detect unhealthy client and evict it (closeClient may throw IOException but should be handled)
        clientCache.get(ENDPOINT_1);

        assertEquals("Cache should be empty after eviction", 0, clientCache.size());

        // Verify that close() was invoked during eviction
        verify(client1, times(1)).close();
    }
}
