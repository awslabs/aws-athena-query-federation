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
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.flight.CallStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    @Test
    public void testIsAuthErrorFlightUnauthenticatedAndUnauthorized()
    {
        assertTrue(InfluxDbConnectionFactory.isAuthError(CallStatus.UNAUTHENTICATED.toRuntimeException()));
        assertTrue(InfluxDbConnectionFactory.isAuthError(CallStatus.UNAUTHORIZED.toRuntimeException()));
    }

    @Test
    public void testIsAuthErrorHttp401And403()
    {
        assertTrue(InfluxDbConnectionFactory.isAuthError(new InfluxDBApiHttpException("unauthorized", null, 401)));
        assertTrue(InfluxDbConnectionFactory.isAuthError(new InfluxDBApiHttpException("forbidden", null, 403)));
    }

    @Test
    public void testIsAuthErrorDetectedInCauseChain()
    {
        final Throwable wrapped = new RuntimeException("wrapper", CallStatus.UNAUTHENTICATED.toRuntimeException());
        assertTrue(InfluxDbConnectionFactory.isAuthError(wrapped));
    }

    @Test
    public void testIsAuthErrorFalseForNonAuthErrors()
    {
        assertFalse(InfluxDbConnectionFactory.isAuthError(new RuntimeException("boom")));
        assertFalse(InfluxDbConnectionFactory.isAuthError(CallStatus.INTERNAL.toRuntimeException()));
        assertFalse(InfluxDbConnectionFactory.isAuthError(new InfluxDBApiHttpException("server error", null, 500)));
    }

    private Map<String, String> baseConfig()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        return config;
    }

    private InfluxDbConnectionFactory spyFactoryReturningClient(final Map<String, String> config)
    {
        final InfluxDBClient mockClient = mock(InfluxDBClient.class);
        final InfluxDbConnectionFactory factory = spy(new InfluxDbConnectionFactory(config, mockHandler));
        doReturn(mockClient).when(factory).getClient(anyString());
        return factory;
    }

    @Test
    public void testExecuteWithTokenRetrySucceedsWithoutRefresh() throws Exception
    {
        final InfluxDbConnectionFactory factory = spyFactoryReturningClient(baseConfig());
        final String result = factory.executeWithTokenRetry("db", client -> "ok");
        assertEquals("ok", result);
        verify(factory, times(1)).getClient("db");
        verify(factory, never()).invalidateToken();
    }

    @Test
    public void testExecuteWithTokenRetryRefreshesOnceThenSucceeds() throws Exception
    {
        final InfluxDbConnectionFactory factory = spyFactoryReturningClient(baseConfig());
        final AtomicInteger calls = new AtomicInteger();
        final String result = factory.executeWithTokenRetry("db", client -> {
            if (calls.getAndIncrement() == 0) {
                throw CallStatus.UNAUTHENTICATED.toRuntimeException();
            }
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(2, calls.get());
        verify(factory, times(2)).getClient("db");
        verify(factory, times(1)).invalidateToken();
    }

    @Test
    public void testExecuteWithTokenRetryExhaustsCapThenThrows()
    {
        final Map<String, String> config = baseConfig();
        config.put("token_refresh_max_retries", "2");
        final InfluxDbConnectionFactory factory = spyFactoryReturningClient(config);
        final AtomicInteger calls = new AtomicInteger();
        try {
            factory.executeWithTokenRetry("db", client -> {
                calls.incrementAndGet();
                throw CallStatus.UNAUTHORIZED.toRuntimeException();
            });
            fail("expected the auth error to propagate after exhausting retries");
        }
        catch (final Exception e) {
            assertTrue(InfluxDbConnectionFactory.isAuthError(e));
        }
        // 1 initial attempt + 2 refresh retries.
        assertEquals(3, calls.get());
        verify(factory, times(2)).invalidateToken();
    }

    @Test
    public void testExecuteWithTokenRetryDoesNotRetryNonAuthError()
    {
        final InfluxDbConnectionFactory factory = spyFactoryReturningClient(baseConfig());
        final AtomicInteger calls = new AtomicInteger();
        try {
            factory.executeWithTokenRetry("db", client -> {
                calls.incrementAndGet();
                throw new RuntimeException("boom");
            });
            fail("expected the non-auth error to propagate");
        }
        catch (final Exception e) {
            assertEquals("boom", e.getMessage());
        }
        assertEquals(1, calls.get());
        verify(factory, never()).invalidateToken();
    }

    @Test
    public void testInvalidateTokenForcesReResolution()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");
        // First resolution returns t1, the next (after invalidation) returns t2.
        when(mockHandler.resolveSecrets("${my-secret}")).thenReturn("t1", "t2");

        final InfluxDbConnectionFactory factory = new InfluxDbConnectionFactory(config, mockHandler);
        assertEquals("t1", factory.resolveToken());
        // Cached — no re-resolution.
        assertEquals("t1", factory.resolveToken());
        factory.invalidateToken();
        // Re-resolved after invalidation.
        assertEquals("t2", factory.resolveToken());
    }
}
