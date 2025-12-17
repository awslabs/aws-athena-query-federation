/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_HBASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HBASE_PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.ZOOKEEPER_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HbaseEnvironmentPropertiesTest
{
    @Test
    public void connectionPropertiesToEnvironment_withValidProperties_returnsEnvironmentMap()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, "localhost",
                HBASE_PORT, "2181",
                ZOOKEEPER_PORT, "2182"
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        assertEquals("Should have one environment variable", 1, environment.size());
        assertEquals(
                "Environment DEFAULT_HBASE value should be host:hbasePort:zookeeperPort",
                "localhost:2181:2182",
                environment.get(DEFAULT_HBASE));
    }

    @Test
    public void connectionPropertiesToEnvironment_withDifferentProperties_returnsEnvironmentMap()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, "hbase.example.com",
                HBASE_PORT, "9090",
                ZOOKEEPER_PORT, "2181"
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        assertEquals(
                "Environment DEFAULT_HBASE value should be host:hbasePort:zookeeperPort",
                "hbase.example.com:9090:2181",
                environment.get(DEFAULT_HBASE));
    }

    @Test
    public void connectionPropertiesToEnvironment_withEmptyMap_returnsMapWithNullPlaceholders()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of();

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        assertEquals("Should have one entry", 1, environment.size());
        String defaultHbase = environment.get(DEFAULT_HBASE);
        assertTrue("DEFAULT_HBASE should contain null when keys missing",
                defaultHbase != null && defaultHbase.contains("null"));
    }

    @Test
    public void connectionPropertiesToEnvironment_withMissingHost_returnsMapWithNullHost()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HBASE_PORT, "2181",
                ZOOKEEPER_PORT, "2182"
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        String defaultHbase = environment.get(DEFAULT_HBASE);
        assertTrue("DEFAULT_HBASE should contain null for missing host",
                defaultHbase != null && defaultHbase.startsWith("null:"));
    }

    @Test
    public void connectionPropertiesToEnvironment_withMissingHbasePort_returnsMapWithNullPort()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, "localhost",
                ZOOKEEPER_PORT, "2182"
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        String defaultHbase = environment.get(DEFAULT_HBASE);
        assertTrue("DEFAULT_HBASE should contain null for missing hbase port",
                defaultHbase != null && defaultHbase.contains("null"));
    }

    @Test
    public void connectionPropertiesToEnvironment_withMissingZookeeperPort_returnsMapWithNullZookeeperPort()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, "localhost",
                HBASE_PORT, "2181"
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        String defaultHbase = environment.get(DEFAULT_HBASE);
        assertTrue("DEFAULT_HBASE should contain null for missing zookeeper port",
                defaultHbase != null && defaultHbase.endsWith(":null"));
    }
}

