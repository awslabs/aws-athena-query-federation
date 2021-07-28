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

public class RedisConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(RedisConnectionFactory.class);

  private static final int CONNECTION_TIMEOUT_MS = 2_000;
  private final Map<String, RedisConnectionWrapper<String, String>> clientCache = new HashMap<>();

  public synchronized RedisConnectionWrapper<String, String> getOrCreateConn(String conStr, boolean sslEnabled,
                                                                             boolean isCluster)
  {
    RedisConnectionWrapper<String, String> connection = clientCache.get(conStr);
    if (connection == null) {
      String[] endpointParts = conStr.split(":");
      if (endpointParts.length == 2) {
        if (isCluster) {
          connection = getOrCreateClusterCon(endpointParts[0], Integer.parseInt(endpointParts[1]), null, sslEnabled);
        }
        else {
          connection = getOrCreateStandaloneCon(endpointParts[0], Integer.parseInt(endpointParts[1]), null, sslEnabled);
        }
      }
      else if (endpointParts.length == 3) {
        if (isCluster) {
          connection = getOrCreateClusterCon(endpointParts[0], Integer.parseInt(endpointParts[1]), endpointParts[2],
                                             sslEnabled);
        }
        else {
          connection = getOrCreateStandaloneCon(endpointParts[0], Integer.parseInt(endpointParts[1]), endpointParts[2],
                                                sslEnabled);
        }
      }
      else {
        throw new IllegalArgumentException("Redis endpoint format error.");
      }
      clientCache.put(conStr, connection);
    }
    return connection;
  }

  private RedisConnectionWrapper<String, String> getOrCreateStandaloneCon(String host, int port, String passwordToken, boolean sslEnabled)
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
