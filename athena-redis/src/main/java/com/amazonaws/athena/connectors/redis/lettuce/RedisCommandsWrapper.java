package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RedisCommandsWrapper<K, V> {

  private final RedisCommands<K, V> standaloneCommands;
  private final RedisAdvancedClusterCommands<K, V> clusterCommands;
  private final boolean isCluster;

  public RedisCommandsWrapper(RedisCommands<K, V> standaloneCommands,
                              RedisAdvancedClusterCommands<K, V> clusterCommands, boolean isCluster)
  {
    this.standaloneCommands = standaloneCommands;
    this.clusterCommands = clusterCommands;
    this.isCluster = isCluster;
    if (isCluster) {
      requireNonNull(clusterCommands, "RedisAdvancedClusterCommands is required");
    }
    else {
      requireNonNull(standaloneCommands, "RedisCommands is required");
    }
  }

  public KeyScanCursor<K> scan(ScanCursor var1, ScanArgs var2)
  {
    if (isCluster) {
      return clusterCommands.scan(var1, var2);
    }
    else {
      return standaloneCommands.scan(var1, var2);
    }
  }

  public Long zcount(K var1, Range<? extends Number> var2)
  {
    if (isCluster) {
      return clusterCommands.zcount(var1, var2);
    }
    else {
      return standaloneCommands.zcount(var1, var2);
    }
  }

  public List<V> zrange(K var1, long var2, long var3)
  {
    if (isCluster) {
      return clusterCommands.zrange(var1, var2, var3);
    }
    else {
      return standaloneCommands.zrange(var1, var2, var3);
    }
  }

  public V get(K var1)
  {
    if (isCluster) {
      return clusterCommands.get(var1);
    }
    else {
      return standaloneCommands.get(var1);
    }
  }

  public Map<K, V> hgetall(K var1)
  {
    if (isCluster) {
      return clusterCommands.hgetall(var1);
    }
    else {
      return standaloneCommands.hgetall(var1);
    }
  }

  public ScoredValueScanCursor<V> zscan(K var1, ScanCursor var2)
  {
    if (isCluster) {
      return clusterCommands.zscan(var1, var2);
    }
    else {
      return standaloneCommands.zscan(var1, var2);
    }
  }
}
