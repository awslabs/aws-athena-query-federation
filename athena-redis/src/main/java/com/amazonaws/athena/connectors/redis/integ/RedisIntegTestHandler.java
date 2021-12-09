/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.redis.integ;

import com.amazonaws.athena.connectors.redis.lettuce.RedisCommandsWrapper;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionFactory;
import com.amazonaws.athena.connectors.redis.lettuce.RedisConnectionWrapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This Lambda function handler is only used within the Redis integration tests. The Lambda function,
 * when invoked, will insert keys into the Redis instances.
 * The invocation of the Lambda function must include the following environment variables:
 * standalone_connection - The connection string used to connect to standalone Redis instance
 * cluster_connection - The connection string used to connect to clustered Redis instance
 */
public class RedisIntegTestHandler
        implements RequestStreamHandler
{
    public static final String HANDLER = "com.amazonaws.athena.connectors.redis.integ.RedisIntegTestHandler";

    private static final Logger logger = LoggerFactory.getLogger(RedisIntegTestHandler.class);

    private static final String STANDALONE_REDIS_DB_NUMBER = "10";

    private final RedisConnectionFactory connectionFactory;
    private final String standaloneConnectionString;
    private final String clusterConnectionString;

    public RedisIntegTestHandler()
    {
        connectionFactory = new RedisConnectionFactory();
        standaloneConnectionString = System.getenv("standalone_connection");
        clusterConnectionString = System.getenv("cluster_connection");
    }

    @Override
    public final void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    {
        logger.info("handleRequest - enter");

        RedisConnectionWrapper<String, String> standaloneConnection = null;
        RedisConnectionWrapper<String, String> clusterConnection = null;
        try {
            standaloneConnection = connectionFactory.getOrCreateConn(standaloneConnectionString, false, false, STANDALONE_REDIS_DB_NUMBER);
            clusterConnection = connectionFactory.getOrCreateConn(clusterConnectionString, true, true);

            insertRedisData(standaloneConnection.sync());
            insertRedisData(clusterConnection.sync());

            logger.info("Keys inserted successfully.");
        }
        catch (Exception e) {
            logger.error("Error setting up Redis table {}", e.getMessage(), e);
        }
        finally {
            if (standaloneConnection != null) {
                standaloneConnection.close();
            }
            if (clusterConnection != null) {
                clusterConnection.close();
            }
        }

        logger.info("handleRequest - exit");
    }

    /**
     * Insert the necessary keys into the provided Redis instance
     */
    private void insertRedisData(RedisCommandsWrapper<String, String> syncCommands)
    {
        /**
         * There are 6 scenarios per redis instance to test:
         * Data Value Type: Hash, Zset(single col, multiple rows), Literal(single col)
         * For each above value type, they can be provided 2 ways: prefix, zset
         */
        // Hash Data (prefix: customer-hm:* | zset: customer-hm-zset)
        Map<String, String> redisMap = new HashMap<>();
        redisMap.put("custkey", "1");
        redisMap.put("name", "Jon Snow");
        redisMap.put("acctbal", "1.00");
        syncCommands.hmset("customer-hm:1", Collections.unmodifiableMap(redisMap));

        redisMap.put("custkey", "2");
        redisMap.put("name", "Robb Stark");
        redisMap.put("acctbal", "2.00");
        syncCommands.hmset("customer-hm:2", Collections.unmodifiableMap(redisMap));

        redisMap.put("custkey", "3");
        redisMap.put("name", "Eddard Stark");
        redisMap.put("acctbal", "3.00");
        syncCommands.hmset("customer-hm:3", Collections.unmodifiableMap(redisMap));

        // Zset Data (prefix: customer-hm-zset* | zset: key-hm-zset)
        // this zset will also be used to access the hashmap keys above
        syncCommands.zadd("customer-hm-zset", 1.0, "customer-hm:1");
        syncCommands.zadd("customer-hm-zset", 2.0, "customer-hm:2");
        syncCommands.zadd("customer-hm-zset", 3.0, "customer-hm:3");
        // another zset to access the above zset
        syncCommands.zadd("key-hm-zset", 1.0, "customer-hm-zset");

        // Literal (prefix: customer-literal:* | zset: key-literal-zset)
        syncCommands.set("customer-literal:1", "Sansa Stark");
        syncCommands.set("customer-literal:2", "Daenerys Targaryen");
        syncCommands.set("customer-literal:3", "Arya Stark");
        // zset to access the above literals
        syncCommands.zadd("key-literal-zset", 1.0, "customer-literal:1");
        syncCommands.zadd("key-literal-zset", 2.0, "customer-literal:2");
        syncCommands.zadd("key-literal-zset", 3.0, "customer-literal:3");
    }
}
