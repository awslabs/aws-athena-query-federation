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

import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.flight.CallStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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

public class InfluxDBConnectionFactoryTest
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

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
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

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
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

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
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

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        assertEquals("plain-secret-value", factory.resolveToken());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testResolveTokenMissingThrows()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        factory.resolveToken();
    }

    @Test
    public void testResolveTokenJsonMissingKeyFallsBackToRaw()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");
        when(mockHandler.resolveSecrets("${my-secret}")).thenReturn("{\"other\": \"value\"}");

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        try {
            factory.resolveToken();
            fail("expected missing key to throw an exception");
        }
        catch (final Exception e) {
            assertTrue(e.getMessage().contains("Unexpected error occurred while parsing secret JSON: JSON secret does not contain key 'token'"));
        }
    }

    @Test
    public void testResolveTokenInvalidJsonFallsBackToRaw() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");
        when(mockHandler.resolveSecrets("${my-secret}")).thenReturn("{not-valid-json");

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        // Invalid JSON will should be treated as a token, since a token may start with '{'..
        factory.resolveToken();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetClientMissingHostThrows() throws ExecutionException
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        factory.getClient("db");
    }

    @Test
    public void testInvalidMaxRetriesConfigFallsBackToDefault() throws Exception
    {
        final Map<String, String> config = baseConfig();
        config.put("token_refresh_max_retries", "not-a-number");
        // Falls back to the default (1) rather than throwing; a single auth error is retried once.
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(config);
        final AtomicInteger calls = new AtomicInteger();
        try {
            factory.executeWithTokenRetry("db", client -> {
                calls.incrementAndGet();
                throw CallStatus.UNAUTHENTICATED.toRuntimeException();
            });
            fail("expected auth error to propagate after the default single retry");
        }
        catch (final Exception e) {
            assertTrue(InfluxDBConnectionFactory.isAuthError(e));
        }
        // Invalid config falls back to the default retry count (not a NumberFormatException at construction).
        assertEquals(1 + InfluxDBConstants.DEFAULT_TOKEN_REFRESH_MAX_RETRIES, calls.get());
    }

    @Test
    public void testResolveDatabaseRestoresOriginalCase() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        config.put("influxdb_database", "MyDatabase");

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        assertEquals("MyDatabase", factory.resolveDatabase("mydatabase"));
    }

    @Test
    public void testIsAuthErrorFlightUnauthenticatedAndUnauthorized()
    {
        assertTrue(InfluxDBConnectionFactory.isAuthError(CallStatus.UNAUTHENTICATED.toRuntimeException()));
        assertTrue(InfluxDBConnectionFactory.isAuthError(CallStatus.UNAUTHORIZED.toRuntimeException()));
    }

    @Test
    public void testIsAuthErrorHttp401And403()
    {
        assertTrue(InfluxDBConnectionFactory.isAuthError(new InfluxDBApiHttpException("unauthorized", null, 401)));
        assertTrue(InfluxDBConnectionFactory.isAuthError(new InfluxDBApiHttpException("forbidden", null, 403)));
    }

    @Test
    public void testIsAuthErrorDetectedInCauseChain()
    {
        final Throwable wrapped = new RuntimeException("wrapper", CallStatus.UNAUTHENTICATED.toRuntimeException());
        assertTrue(InfluxDBConnectionFactory.isAuthError(wrapped));
    }

    @Test
    public void testIsAuthErrorFalseForNonAuthErrors()
    {
        assertFalse(InfluxDBConnectionFactory.isAuthError(new RuntimeException("boom")));
        assertFalse(InfluxDBConnectionFactory.isAuthError(CallStatus.INTERNAL.toRuntimeException()));
        assertFalse(InfluxDBConnectionFactory.isAuthError(new InfluxDBApiHttpException("server error", null, 500)));
    }

    @Test
    public void testIsThrottleForFlightResourceExhaustedAndHttp429()
    {
        assertTrue(InfluxDBConnectionFactory.isThrottle(CallStatus.RESOURCE_EXHAUSTED.toRuntimeException()));
        assertTrue(InfluxDBConnectionFactory.isThrottle(new InfluxDBApiHttpException("slow down", null, 429)));
        assertTrue(InfluxDBConnectionFactory.isThrottle(
                new RuntimeException("wrap", CallStatus.RESOURCE_EXHAUSTED.toRuntimeException())));
    }

    @Test
    public void testIsThrottleFalseForOtherErrors()
    {
        assertFalse(InfluxDBConnectionFactory.isThrottle(new RuntimeException("boom")));
        assertFalse(InfluxDBConnectionFactory.isThrottle(CallStatus.UNAUTHENTICATED.toRuntimeException()));
        assertFalse(InfluxDBConnectionFactory.isThrottle(new InfluxDBApiHttpException("forbidden", null, 403)));
    }

    @Test
    public void testExecuteWithTokenRetrySurfacesThrottleAsFederationThrottleException() throws ExecutionException
    {
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(baseConfig());
        try {
            factory.executeWithTokenRetry("db", client -> {
                throw CallStatus.RESOURCE_EXHAUSTED.toRuntimeException();
            });
            fail("expected throttle to surface as FederationThrottleException");
        }
        catch (final Exception e) {
            assertTrue(e instanceof FederationThrottleException);
        }
    }

    private Map<String, String> baseConfig()
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_HOST_URL", "https://localhost:8086");
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        return config;
    }

    private InfluxDBConnectionFactory spyFactoryReturningClient(final Map<String, String> config) throws ExecutionException
    {
        final InfluxDBClient mockClient = mock(InfluxDBClient.class);
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(config, mockHandler));
        doReturn(mockClient).when(factory).getClient(anyString());
        return factory;
    }

    @Test
    public void testExecuteWithTokenRetrySucceedsWithoutRefresh() throws Exception
    {
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(baseConfig());
        final String result = factory.executeWithTokenRetry("db", client -> "ok");
        assertEquals("ok", result);
        verify(factory, times(1)).getClient("db");
        verify(factory, never()).invalidateToken();
    }

    @Test
    public void testExecuteWithTokenRetryRefreshesOnceThenSucceeds() throws Exception
    {
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(baseConfig());
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
    public void testExecuteWithTokenRetryExhaustsCapThenThrows() throws ExecutionException
    {
        final Map<String, String> config = baseConfig();
        config.put("token_refresh_max_retries", "2");
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(config);
        final AtomicInteger calls = new AtomicInteger();
        try {
            factory.executeWithTokenRetry("db", client -> {
                calls.incrementAndGet();
                throw CallStatus.UNAUTHORIZED.toRuntimeException();
            });
            fail("expected the auth error to propagate after exhausting retries");
        }
        catch (final Exception e) {
            assertTrue(InfluxDBConnectionFactory.isAuthError(e));
        }
        // 1 initial attempt + 2 refresh retries.
        assertEquals(3, calls.get());
        verify(factory, times(2)).invalidateToken();
    }

    @Test
    public void testExecuteWithTokenRetryDoesNotRetryNonAuthError() throws ExecutionException
    {
        final InfluxDBConnectionFactory factory = spyFactoryReturningClient(baseConfig());
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

        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
        assertEquals("t1", factory.resolveToken());
        // Cached — no re-resolution.
        assertEquals("t1", factory.resolveToken());
        factory.invalidateToken();
        // Re-resolved after invalidation.
        assertEquals("t2", factory.resolveToken());
    }
}
