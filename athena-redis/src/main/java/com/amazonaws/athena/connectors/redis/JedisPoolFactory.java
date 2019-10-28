/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates and Caches JedisPool Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be host:port or host:port:password_token
 */
public class JedisPoolFactory
{
    private static final Logger logger = LoggerFactory.getLogger(JedisPoolFactory.class);

    //Realistically we wouldn't need more than 1 but using 4 to give the pool some wiggle room for
    //connections that are dying / starting to avoid impacting getting a connection quickly.
    private static final int MAX_CONS = 4;
    private static final int CONNECTION_TIMEOUT_MS = 2_000;

    private final Map<String, JedisPool> clientCache = new HashMap<>();

    /**
     * Gets or Creates a Jedis instance for the given connection string.
     * @param conStr Redis connection details, format is expected to be host:port or host:port:password_token
     * @return A Jedis connection if the connection succeeded, else the function will throw.
     */
    public synchronized Jedis getOrCreateConn(String conStr)
    {
        JedisPool pool = clientCache.get(conStr);
        if (pool == null) {
            String[] endpointParts = conStr.split(":");
            if (endpointParts.length == 2) {
                pool = getOrCreateCon(endpointParts[0], Integer.valueOf(endpointParts[1]));
            }
            else if (endpointParts.length == 3) {
                pool = getOrCreateCon(endpointParts[0], Integer.valueOf(endpointParts[1]), endpointParts[2]);
            }
            else {
                throw new IllegalArgumentException("Redis endpoint format error.");
            }

            clientCache.put(conStr, pool);
        }
        return pool.getResource();
    }

    private JedisPool getOrCreateCon(String host, int port)
    {
        logger.info("getOrCreateCon: Creating connection pool.");
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_CONS);
        return new JedisPool(poolConfig, host, port, CONNECTION_TIMEOUT_MS);
    }

    private JedisPool getOrCreateCon(String host, int port, String passwordToken)
    {
        logger.info("getOrCreateCon: Creating connection pool with password.");
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_CONS);
        return new JedisPool(poolConfig, host, port, CONNECTION_TIMEOUT_MS, passwordToken);
    }
}
