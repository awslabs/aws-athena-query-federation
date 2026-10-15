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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisConnectionFactoryTest
{
    private RedisConnectionFactory factory;
    
    private MockedStatic<RedisClient> redisClientStatic;
    private MockedStatic<RedisClusterClient> redisClusterClientStatic;
    
    @Before
    public void setUp()
    {
        factory = new RedisConnectionFactory();
    }
    
    @After
    public void tearDown()
    {
        if (redisClientStatic != null) {
            redisClientStatic.close();
        }
        if (redisClusterClientStatic != null) {
            redisClusterClientStatic.close();
        }
    }
    
    @Test
    public void getOrCreateConn_withStandaloneRedis_returnsConnection()
    {
        redisClientStatic = mockStatic(RedisClient.class);
        
        RedisClient mockClient = mock(RedisClient.class);
        StatefulRedisConnection<String, String> mockConnection = mock(StatefulRedisConnection.class);
        RedisCommands<String, String> mockCommands = mock(RedisCommands.class);
        
        redisClientStatic.when(() -> RedisClient.create(any(RedisURI.class)))
                .thenReturn(mockClient);
        when(mockClient.connect()).thenReturn(mockConnection);
        when(mockConnection.sync()).thenReturn(mockCommands);
        
        RedisConnectionWrapper<String, String> wrapper = factory.getOrCreateConn("localhost:6379", false, false);
        
        assertNotNull(wrapper);
        verify(mockClient, times(1)).connect();
        verify(mockConnection, times(1)).sync();
    }

    @Test
    public void getOrCreateConn_withClusterRedis_returnsConnection()
    {
        redisClusterClientStatic = mockStatic(RedisClusterClient.class);
        
        RedisClusterClient mockClusterClient = mock(RedisClusterClient.class);
        StatefulRedisClusterConnection<String, String> mockClusterConnection = mock(StatefulRedisClusterConnection.class);
        RedisAdvancedClusterCommands<String, String> mockCommands = mock(RedisAdvancedClusterCommands.class);
        
        redisClusterClientStatic.when(() -> RedisClusterClient.create(any(RedisURI.class)))
                .thenReturn(mockClusterClient);
        when(mockClusterClient.connect()).thenReturn(mockClusterConnection);
        when(mockClusterConnection.sync()).thenReturn(mockCommands);
        
        RedisConnectionWrapper<String, String> wrapper = factory.getOrCreateConn("localhost:6379", false, true, null);
        
        assertNotNull(wrapper);
        verify(mockClusterClient, times(1)).connect();
        verify(mockClusterConnection, times(1)).sync();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateConn_withInvalidEndpoint_throwsException()
    {
        factory.getOrCreateConn("invalid_endpoint", false, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getOrCreateConn_withEmptyEndpoint_throwsException()
    {
        factory.getOrCreateConn("", false, false);
    }
    
    @Test(expected = NumberFormatException.class)
    public void getOrCreateConn_withNonNumericPort_throwsNumberFormatException()
    {
        factory.getOrCreateConn("localhost:notaport", false, false);
    }
}
