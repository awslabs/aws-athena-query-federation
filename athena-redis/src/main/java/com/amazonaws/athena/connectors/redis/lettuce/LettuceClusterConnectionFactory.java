package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LettuceClusterConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(LettuceClusterConnectionFactory.class);

  private static final int CONNECTION_TIMEOUT_MS = 2_000;

  private StatefulRedisClusterConnection<String, String> getOrCreateCon(String host, int port, boolean sslEnabled)
  {
    logger.info("getOrCreateCon: Creating connection.");
    RedisURI redisUri = RedisURI.Builder
                                .redis(host, port)
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS))
                                .build();
    RedisClusterClient clusterClient = RedisClusterClient.create(redisUri);
    StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
    connection.setReadFrom(ReadFrom.UPSTREAM_PREFERRED);
    return connection;
  }

  private StatefulRedisClusterConnection<String, String> getOrCreateCon(String host, int port, String passwordToken, boolean sslEnabled)
  {
    logger.info("getOrCreateCon: Creating connection with password.");
    RedisURI redisUri = RedisURI.Builder
                                .redis(host, port)
                                .withPassword(passwordToken.toCharArray())
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS))
                                .build();
    RedisClusterClient clusterClient = RedisClusterClient.create(redisUri);
    StatefulRedisClusterConnection<String, String> connection = clusterClient.connect();
    connection.setReadFrom(ReadFrom.UPSTREAM_PREFERRED);
    return connection;
  }
}
