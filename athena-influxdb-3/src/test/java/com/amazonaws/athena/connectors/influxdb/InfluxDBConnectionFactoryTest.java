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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.FederationThrottleException;
import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.flight.CallStatus;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
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
    public void testSetHandlerIsUsedForSecretResolution()
    {
        final Map<String, String> config = baseConfig();
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, null);
        factory.setHandler(mockHandler);
        assertEquals("my-plain-token", factory.resolveToken());
        verify(mockHandler, times(1)).resolveSecrets("my-plain-token");
    }

    @Test
    public void testGetClientUsesConfiguredDefaultAndCachesClient() throws Exception
    {
        final Map<String, String> config = baseConfig();
        config.put("influxdb_database", "MyDb");
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(config, mockHandler));
        doReturn(List.of(new InfluxDBConnectionFactory.DatabaseInfo("MyDb"))).when(factory).listDatabases();

        // null and empty fall back to the configured default database.
        final InfluxDBClient first = factory.getClient(null);
        assertNotNull(first);
        assertSame(first, factory.getClient(""));
        assertSame(first, factory.getClient("MyDb"));
        // Existence is only verified when a client is minted, not on cache hits.
        verify(factory, times(1)).listDatabases();

        // Closing evicts the cached client; the next call mints (and re-verifies) a new one.
        factory.closeAllClients();
        final InfluxDBClient second = factory.getClient("MyDb");
        assertNotNull(second);
        assertNotSame(first, second);
        verify(factory, times(2)).listDatabases();
        factory.closeAllClients();
    }

    @Test
    public void testGetClientDeniesDatabaseOutsideConfiguredScope() throws Exception
    {
        final Map<String, String> config = baseConfig();
        config.put("influxdb_database", "MyDb");
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(config, mockHandler));
        try {
            factory.getClient("OtherDb");
            fail("expected access to an out-of-scope database to be denied");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("scoped to 'MyDb'"));
        }
        verify(factory, never()).listDatabases();
    }

    @Test
    public void testGetClientWithoutDatabaseOrDefaultThrows()
    {
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(baseConfig(), mockHandler);
        try {
            factory.getClient(null);
            fail("expected missing database to throw");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No database specified"));
        }
    }

    @Test
    public void testGetClientRejectsNonExistentDatabase() throws Exception
    {
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(baseConfig(), mockHandler));
        doReturn(Arrays.asList(new InfluxDBConnectionFactory.DatabaseInfo("Other"), null)).when(factory).listDatabases();
        try {
            factory.getClient("MyDb");
            fail("expected a non-existent database to be rejected");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Database does not exist: 'MyDb'"));
        }
    }

    @Test
    public void testGetClientWrapsListDatabasesFailures() throws Exception
    {
        when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(baseConfig(), mockHandler));

        doThrow(new IOException("io")).when(factory).listDatabases();
        try {
            factory.getClient("MyDb");
            fail("expected IOException to be wrapped");
        }
        catch (final RuntimeException e) {
            assertTrue(e.getMessage().contains("Failed to verify database 'MyDb' exists"));
            assertTrue(e.getCause() instanceof IOException);
        }

        doThrow(new InterruptedException("interrupted")).when(factory).listDatabases();
        try {
            factory.getClient("MyDb");
            fail("expected InterruptedException to be wrapped");
        }
        catch (final RuntimeException e) {
            assertTrue(e.getMessage().contains("Interrupted while verifying database 'MyDb' exists"));
            assertTrue(Thread.interrupted()); // also clears the flag so later tests are unaffected
        }
    }

    @Test
    public void testResolveDatabaseFallsBackToServerListAndFailsClosed() throws Exception
    {
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(baseConfig(), mockHandler));
        doReturn(Arrays.asList(null, new InfluxDBConnectionFactory.DatabaseInfo("MyDb"))).when(factory).listDatabases();

        assertEquals("MyDb", factory.resolveDatabase("mydb"));
        try {
            factory.resolveDatabase("missing");
            fail("expected an unknown schema to be rejected");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Database not found: 'missing'"));
        }
    }

    @Test
    public void testResolveTableNameRestoresCaseFromInformationSchema() throws Exception
    {
        final InfluxDBClient mockClient = mock(InfluxDBClient.class);
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(baseConfig(), mockHandler));
        doReturn(mockClient).when(factory).getClient(anyString());
        when(mockClient.query(anyString(), anyMap())).thenReturn(Stream.<Object[]>of(new Object[] {"MyTable"}));

        assertEquals("MyTable", factory.resolveTableName("MyDb", new TableName("mydb", "mytable")));
        verify(mockClient).query(anyString(), eq(Map.of("table_name", "mytable")));
    }

    @Test
    public void testResolveTableNameThrowsWhenTableMissing() throws Exception
    {
        final InfluxDBClient mockClient = mock(InfluxDBClient.class);
        final InfluxDBConnectionFactory factory = spy(new InfluxDBConnectionFactory(baseConfig(), mockHandler));
        doReturn(mockClient).when(factory).getClient(anyString());
        when(mockClient.query(anyString(), anyMap())).thenReturn(Stream.empty());
        try {
            factory.resolveTableName("MyDb", new TableName("mydb", "nope"));
            fail("expected a missing table to throw");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Table not found in database 'MyDb': nope"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testListDatabasesMissingHostThrows() throws Exception
    {
        final Map<String, String> config = new HashMap<>();
        config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
        new InfluxDBConnectionFactory(config, mockHandler).listDatabases();
    }

    @Test
    public void testListDatabasesRejectsPlainHttpUnlessAllowed() throws Exception
    {
        final Map<String, String> config = baseConfig();
        config.put("INFLUXDB3_HOST_URL", "http://localhost:8086");
        try {
            new InfluxDBConnectionFactory(config, mockHandler).listDatabases();
            fail("expected plain http to be rejected");
        }
        catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Host must use HTTPS"));
        }
    }

    @Test
    public void testListDatabasesParsesResponse() throws Exception
    {
        final List<Integer> statuses = new ArrayList<>();
        final List<String> authHeaders = new ArrayList<>();
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> {
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            statuses.add(200);
            return new String[] {"200", "[{\"iox::database\":\"DbOne\"},{\"iox::database\":\"DbTwo\"}]"};
        })) {
            when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(server.config(), mockHandler);
            final List<InfluxDBConnectionFactory.DatabaseInfo> databases = factory.listDatabases();
            assertEquals(2, databases.size());
            assertEquals("DbOne", databases.get(0).name);
            assertEquals("DbTwo", databases.get(1).name);
            assertEquals(List.of("Bearer my-plain-token"), authHeaders);
            assertEquals("/api/v3/configure/database", server.lastPath());
        }
    }

    @Test
    public void testListDatabasesTreatsNullBodyAsEmpty() throws Exception
    {
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> new String[] {"200", "null"})) {
            when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(server.config(), mockHandler);
            assertTrue(factory.listDatabases().isEmpty());
        }
    }

    @Test
    public void testListDatabasesRefreshesTokenOnAuthErrorThenSucceeds() throws Exception
    {
        final List<String> authHeaders = new ArrayList<>();
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> {
            final String auth = exchange.getRequestHeaders().getFirst("Authorization");
            authHeaders.add(auth);
            return "Bearer t2".equals(auth)
                    ? new String[] {"200", "[{\"iox::database\":\"Db\"}]"}
                    : new String[] {"401", "unauthorized"};
        })) {
            final Map<String, String> config = server.config();
            config.put("INFLUXDB3_AUTH_TOKEN", "${my-secret}");
            when(mockHandler.resolveSecrets("${my-secret}")).thenReturn("t1", "t2");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
            final List<InfluxDBConnectionFactory.DatabaseInfo> databases = factory.listDatabases();
            assertEquals(1, databases.size());
            assertEquals("Db", databases.get(0).name);
            assertEquals(List.of("Bearer t1", "Bearer t2"), authHeaders);
        }
    }

    @Test
    public void testListDatabasesGivesUpAfterRetryCapOnAuthError() throws Exception
    {
        final AtomicInteger requests = new AtomicInteger();
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> {
            requests.incrementAndGet();
            return new String[] {"403", "forbidden"};
        })) {
            final Map<String, String> config = server.config();
            config.put("token_refresh_max_retries", "1");
            when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(config, mockHandler);
            try {
                factory.listDatabases();
                fail("expected a persistent auth failure to propagate");
            }
            catch (final RuntimeException e) {
                assertTrue(e.getMessage().contains("status code: 403"));
            }
            // 1 initial attempt + 1 refresh retry.
            assertEquals(2, requests.get());
        }
    }

    @Test
    public void testListDatabasesSurfaces429AsThrottle() throws Exception
    {
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> new String[] {"429", "slow down"})) {
            when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(server.config(), mockHandler);
            try {
                factory.listDatabases();
                fail("expected 429 to surface as FederationThrottleException");
            }
            catch (final FederationThrottleException e) {
                assertTrue(e.getMessage().contains("throttled"));
            }
        }
    }

    @Test
    public void testListDatabasesFailsOnUnexpectedStatus() throws Exception
    {
        try (LocalInfluxServer server = new LocalInfluxServer(exchange -> new String[] {"500", "boom"})) {
            when(mockHandler.resolveSecrets("my-plain-token")).thenReturn("my-plain-token");
            final InfluxDBConnectionFactory factory = new InfluxDBConnectionFactory(server.config(), mockHandler);
            try {
                factory.listDatabases();
                fail("expected a 500 to fail");
            }
            catch (final RuntimeException e) {
                assertTrue(e.getMessage().contains("status code: 500"));
            }
        }
    }

    /**
     * Minimal local HTTP server standing in for the InfluxDB {@code /api/v3/configure/database} endpoint. The
     * responder returns {@code {statusCode, body}} for each request.
     */
    private static final class LocalInfluxServer implements AutoCloseable
    {
        private final HttpServer server;
        private volatile String lastPath;

        LocalInfluxServer(final Function<HttpExchange, String[]> responder) throws IOException
        {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                lastPath = exchange.getRequestURI().getPath();
                final String[] response = responder.apply(exchange);
                final byte[] body = response[1].getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(Integer.parseInt(response[0]), body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            });
            server.start();
        }

        Map<String, String> config()
        {
            final Map<String, String> config = new HashMap<>();
            config.put("INFLUXDB3_HOST_URL", "http://127.0.0.1:" + server.getAddress().getPort());
            config.put("INFLUXDB3_AUTH_TOKEN", "my-plain-token");
            config.put("ALLOW_INSECURE_TRANSPORT", "true");
            return config;
        }

        String lastPath()
        {
            return lastPath;
        }

        @Override
        public void close()
        {
            server.stop(0);
        }
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
