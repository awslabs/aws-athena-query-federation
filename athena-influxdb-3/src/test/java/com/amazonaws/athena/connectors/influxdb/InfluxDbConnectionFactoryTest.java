/*-
 * #%L
 * athena-influxdb
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
package com.amazonaws.athena.connectors.influxdb;

import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InfluxDbConnectionFactoryTest
{
    private FederationRequestHandler mockHandler;

    @Before
    public void setUp()
    {
        mockHandler = mock(FederationRequestHandler.class);
    }

    @Test
    public void testResolveTokenPlainString()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");

        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("my-plain-token", factory.resolveToken());
    }

    @Test
    public void testResolveTokenJsonWithDefaultKey()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");

        when(mockHandler.resolveSecrets("${my-secret}"))
                .thenReturn("{\"token\": \"secret-token-value\", \"other\": \"stuff\"}");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("secret-token-value", factory.resolveToken());
    }

    @Test
    public void testResolveTokenJsonWithCustomKey()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");
        config.put("INFLUXDB3_AUTH_TOKEN_KEY", "api_key");

        when(mockHandler.resolveSecrets("${my-secret}"))
                .thenReturn("{\"api_key\": \"custom-key-value\"}");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("custom-key-value", factory.resolveToken());
    }

    @Test
    public void testResolveTokenSecretsManagerPlainString()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");

        // Secrets Manager returns a plain string, not JSON
        when(mockHandler.resolveSecrets("${my-secret}")).thenReturn("plain-secret-value");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("plain-secret-value", factory.resolveToken());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveTokenMissingThrows()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        factory.resolveToken();
    }

    @Test
    public void testResolveDatabaseRestoresOriginalCase() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        config.put("influxdb_database", "MyDatabase");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("MyDatabase", factory.resolveDatabase("mydatabase"));
    }
}
