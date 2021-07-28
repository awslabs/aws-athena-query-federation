package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import static java.util.Objects.requireNonNull;

public class RedisConnectionWrapper<K, V> {

  private final StatefulRedisConnection<K, V> standaloneConnection;
  private final StatefulRedisClusterConnection<K, V> clusterConnection;
  private final boolean isCluster;
  private final RedisCommandsWrapper<K, V> redisCommandsWrapper;

  public RedisConnectionWrapper(StatefulRedisConnection<K, V> standaloneConnection,
                                StatefulRedisClusterConnection<K, V> clusterConnection, boolean isCluster) {
    this.standaloneConnection = standaloneConnection;
    this.clusterConnection = clusterConnection;
    this.isCluster = isCluster;
    if (isCluster) {
      requireNonNull(clusterConnection, "Cluster Connection is required");
      redisCommandsWrapper = new RedisCommandsWrapper<K, V>(null, clusterConnection.sync(), isCluster);
    }
    else {
      requireNonNull(standaloneConnection, "Standalone Connection is required");
      redisCommandsWrapper = new RedisCommandsWrapper<K, V>(standaloneConnection.sync(), null, isCluster);
    }
  }

  public RedisCommandsWrapper<K, V> sync()
  {
    return this.redisCommandsWrapper;
  }
}
