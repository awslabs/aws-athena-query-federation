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

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.arrow.util.VisibleForTesting;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RedisCommandsWrapper<K, V>
{
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

  public <T> T evalReadOnly(byte[] script, ScriptOutputType type, K[] keys, V... values)
  {
    if (isCluster) {
      return clusterCommands.evalReadOnly(script, type, keys, values);
    }
    else {
      return standaloneCommands.evalReadOnly(script, type, keys, values);
    }
  }

  @VisibleForTesting
  public String hmset(K var1, Map<K, V> var2)
  {
    if (isCluster) {
      return clusterCommands.hmset(var1, var2);
    }
    else {
      return standaloneCommands.hmset(var1, var2);
    }
  }

  @VisibleForTesting
  public Long zadd(K var1, double var2, V var3)
  {
    if (isCluster) {
      return clusterCommands.zadd(var1, var2, var3);
    }
    else {
      return standaloneCommands.zadd(var1, var2, var3);
    }
  }

  @VisibleForTesting
  public String set(K var1, V var2)
  {
    if (isCluster) {
      return clusterCommands.set(var1, var2);
    }
    else {
      return standaloneCommands.set(var1, var2);
    }
  }
}
