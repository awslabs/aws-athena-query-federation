/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.redis.RedisMetadataHandler.DEFAULT_REDIS_DB_NUMBER;

public class RedisConnectionFactory
{
  private static final Logger logger = LoggerFactory.getLogger(RedisConnectionFactory.class);

  private static final int CONNECTION_TIMEOUT_MS = 2_000;
  private final Map<String, RedisConnectionWrapper<String, String>> clientCache = new HashMap<>();

  public synchronized RedisConnectionWrapper<String, String> getOrCreateConn(String conStr,
                                                                             boolean sslEnabled,
                                                                             boolean isCluster)
  {
    return getOrCreateConn(conStr, sslEnabled, isCluster, DEFAULT_REDIS_DB_NUMBER);
  }

  public synchronized RedisConnectionWrapper<String, String> getOrCreateConn(String conStr,
                                                                             boolean sslEnabled,
                                                                             boolean isCluster,
                                                                             String dbNumber)
  {
    String conKey = conStr + sslEnabled + isCluster;
    if (!isCluster && dbNumber != null) {
      conKey += dbNumber;
    }
    RedisConnectionWrapper<String, String> connection = clientCache.get(conKey);
    if (connection == null) {
      String[] endpointParts = conStr.split(":");
      if (endpointParts.length == 2) {
        if (isCluster) {
          connection = getOrCreateClusterCon(endpointParts[0], Integer.parseInt(endpointParts[1]), null,
                                             sslEnabled);
        }
        else {
          connection = getOrCreateStandaloneCon(endpointParts[0], Integer.parseInt(endpointParts[1]), null,
                                                sslEnabled, dbNumber);
        }
      }
      else if (endpointParts.length == 3) {
        if (isCluster) {
          connection = getOrCreateClusterCon(endpointParts[0], Integer.parseInt(endpointParts[1]), endpointParts[2],
                                             sslEnabled);
        }
        else {
          connection = getOrCreateStandaloneCon(endpointParts[0], Integer.parseInt(endpointParts[1]), endpointParts[2],
                                                sslEnabled, dbNumber);
        }
      }
      else {
        throw new IllegalArgumentException("Redis endpoint format error.");
      }
      clientCache.put(conKey, connection);
    }
    return connection;
  }

  private RedisConnectionWrapper<String, String> getOrCreateStandaloneCon(String host, int port, String passwordToken, boolean sslEnabled, String dbNumber)
  {
    logger.info("getOrCreateCon: Creating Standalone connection");
    if (passwordToken != null) {
      logger.info("getOrCreateCon: With Password");
    }
    if (sslEnabled) {
      logger.info("getOrCreateCon: With SSL");
    }
    RedisURI.Builder redisUriBuilder = RedisURI.Builder
                                .redis(host, port)
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS));
    if (passwordToken != null) {
      redisUriBuilder.withPassword(passwordToken.toCharArray());
    }
    if (dbNumber != null && !dbNumber.isEmpty()) {
      logger.info("getOrCreateCon: With DB Number {}", dbNumber);
      redisUriBuilder.withDatabase(Integer.parseInt(dbNumber));
    }
    RedisClient client = RedisClient.create(redisUriBuilder.build());
    return new RedisConnectionWrapper<>(client.connect(), null, false);
  }

  private RedisConnectionWrapper<String, String> getOrCreateClusterCon(String host, int port, String passwordToken, boolean sslEnabled)
  {
    logger.info("getOrCreateCon: Creating Cluster connection");
    if (passwordToken != null) {
      logger.info("getOrCreateCon: With Password");
    }
    if (sslEnabled) {
      logger.info("getOrCreateCon: With SSL");
    }
    RedisURI.Builder redisUriBuilder = RedisURI.Builder
                                .redis(host, port)
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS));
    if (passwordToken != null) {
      redisUriBuilder.withPassword(passwordToken.toCharArray());
    }
    RedisClusterClient clusterClient = RedisClusterClient.create(redisUriBuilder.build());
    StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
    connection.setReadFrom(ReadFrom.UPSTREAM_PREFERRED);
    return new RedisConnectionWrapper<>(null, connection, true);
  }
}
