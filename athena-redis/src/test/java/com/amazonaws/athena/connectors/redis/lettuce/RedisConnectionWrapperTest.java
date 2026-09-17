/*-
 * #%L
 * athena-redis
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisConnectionWrapperTest
{
    @Mock
    private StatefulRedisConnection<String, String> standaloneConnection;
    @Mock
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    @Mock
    private RedisCommands<String, String> redisCommands;
    @Mock
    private RedisAdvancedClusterCommands<String, String> clusterCommands;

    private RedisConnectionWrapper<String, String> standaloneWrapper;
    private RedisConnectionWrapper<String, String> clusterWrapper;

    @Before
    public void setUp()
    {
        when(standaloneConnection.sync()).thenReturn(redisCommands);
        when(clusterConnection.sync()).thenReturn(clusterCommands);

        standaloneWrapper = new RedisConnectionWrapper<>(standaloneConnection, null, false);
        clusterWrapper = new RedisConnectionWrapper<>(null, clusterConnection, true);
    }

    @Test
    public void sync_withStandaloneConnection_returnsWrapper()
    {
        RedisCommandsWrapper<String, String> wrapper = standaloneWrapper.sync();
        assertNotNull(wrapper);
    }

    @Test
    public void sync_withClusterConnection_returnsWrapper()
    {
        RedisCommandsWrapper<String, String> wrapper = clusterWrapper.sync();
        assertNotNull(wrapper);
    }

    @Test
    public void close_withStandaloneConnection_closesConnection()
    {
        standaloneWrapper.close();
        verify(standaloneConnection, times(1)).close();
    }

    @Test
    public void close_withClusterConnection_closesConnection()
    {
        clusterWrapper.close();
        verify(clusterConnection, times(1)).close();
    }
}
