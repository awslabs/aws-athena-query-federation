package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LettuceStandaloneConnectionFactory {
  private static final Logger logger = LoggerFactory.getLogger(LettuceStandaloneConnectionFactory.class);

  private static final int CONNECTION_TIMEOUT_MS = 2_000;

  private StatefulRedisConnection<String, String> getOrCreateCon(String host, int port, boolean sslEnabled)
  {
    logger.info("getOrCreateCon: Creating connection.");
    RedisURI redisUri = RedisURI.Builder
                                .redis(host, port)
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS))
                                .build();
    RedisClient client = RedisClient.create(redisUri);
    return client.connect();
  }

  private StatefulRedisConnection<String, String> getOrCreateCon(String host, int port, String passwordToken, boolean sslEnabled)
  {
    logger.info("getOrCreateCon: Creating connection with password.");
    RedisURI redisUri = RedisURI.Builder
                                .redis(host, port)
                                .withPassword(passwordToken.toCharArray())
                                .withSsl(sslEnabled)
                                .withTimeout(Duration.ofMillis(CONNECTION_TIMEOUT_MS))
                                .build();
    RedisClient client = RedisClient.create(redisUri);
    return client.connect();
  }
}
