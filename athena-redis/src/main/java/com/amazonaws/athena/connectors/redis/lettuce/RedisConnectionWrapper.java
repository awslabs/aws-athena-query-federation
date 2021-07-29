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

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import static java.util.Objects.requireNonNull;

public class RedisConnectionWrapper<K, V>
{
  private final StatefulRedisConnection<K, V> standaloneConnection;
  private final StatefulRedisClusterConnection<K, V> clusterConnection;
  private final boolean isCluster;
  private final RedisCommandsWrapper<K, V> redisCommandsWrapper;

  public RedisConnectionWrapper(StatefulRedisConnection<K, V> standaloneConnection,
                                StatefulRedisClusterConnection<K, V> clusterConnection, boolean isCluster)
  {
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
