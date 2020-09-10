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
package com.amazonaws.connectors.athena.elasticsearch;

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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the CacheableAwsRestHighLevelClient class.
 */
@RunWith(MockitoJUnitRunner.class)
public class CacheableAwsRestHighLevelClientTest
{
    private static final Logger logger = LoggerFactory.getLogger(CacheableAwsRestHighLevelClientTest.class);

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
    public void getClientSimpleTest()
            throws IOException
    {
        logger.info("getClientSimpleTest - enter");

        clientCache.put("endpoint1", client1);
        clientCache.put("endpoint2", client2);

        when(client1.ping(RequestOptions.DEFAULT)).thenReturn(true);
        when(client2.ping(RequestOptions.DEFAULT)).thenReturn(true);

        assertEquals("Cache has wrong number of clients.", 2, clientCache.size());
        assertNotNull("Cache get() method returned null.", clientCache.get("endpoint1"));
        assertNotNull("Cache get() method returned null.", clientCache.get("endpoint2"));
        assertEquals("Wrong client returned from cache.", client1, clientCache.get("endpoint1"));
        assertEquals("Wrong client returned from cache.", client2, clientCache.get("endpoint2"));

        logger.info("getClientSimpleTest - exit");
    }

    /**
     * Test the cache's ability to evict clients that have aged out (as indicated by MAX_CACHE_AGE_MS).
     */
    @Test
    public void evictOldClientTest()
            throws IOException
    {
        logger.info("evictOldClientTest - enter");

        // Test eviction on the get() method.
        clientCache.addClientEntry("endpoint1", client1, System.currentTimeMillis() -
                CacheableAwsRestHighLevelClient.MAX_CACHE_AGE_MS - 1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get("endpoint1"));
        assertEquals("Cache has wrong number of clients.", 0, clientCache.size());

        // Test eviction on the put() method.
        when(client2.ping(any())).thenReturn(true);
        clientCache.addClientEntry("endpoint1", client1, System.currentTimeMillis() -
                CacheableAwsRestHighLevelClient.MAX_CACHE_AGE_MS - 1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());

        clientCache.put("endpoint2", client2);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNotNull("Cache get() method returned null.", clientCache.get("endpoint2"));
        assertEquals("Wrong client returned from cache.", client2, clientCache.get("endpoint2"));

        logger.info("evictOldClientTest - exit");
    }

    /**
     * Test the cache's ability to evict unhealthy clients.
     */
    @Test
    public void evictUnhealthyClientTest()
            throws IOException
    {
        logger.info("evictUnhealthyClientTest - enter");

        when(client1.ping(any())).thenReturn(false);
        clientCache.put("endpoint1", client1);

        assertEquals("Cache has wrong number of clients.", 1, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get("endpoint1"));
        assertEquals("Cache has wrong number of clients.", 0, clientCache.size());

        logger.info("evictUnhealthyClientTest - exit");
    }

    /**
     * Test the cache's ability to evict the oldest clients when the cache's capacity is exceeded (as indicated by
     * MAX_CACHE_SIZE).
     */
    @Test
    public void evictExceededCapacityClientTest()
    {
        logger.info("evictExceededCapacityClientTest - enter");

        clientCache.put("endpoint1", client1);
        for (int i = 0; i < CacheableAwsRestHighLevelClient.MAX_CACHE_SIZE; ++i) {
            String endpoint = "newendpoint" + i;
            clientCache.put(endpoint, client2);
        }

        assertEquals("Cache has wrong number of clients.",
                CacheableAwsRestHighLevelClient.MAX_CACHE_SIZE, clientCache.size());
        assertNull("Invalid value returned from cache.", clientCache.get("endpoint1"));

        logger.info("evictExceededCapacityClientTest - exit");
    }
}
