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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.DEFAULT_TOKEN_KEY;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.DEFAULT_TOKEN_REFRESH_MAX_RETRIES;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.ENV_ALLOW_INSECURE_TRANSPORT;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.ENV_INFLUXDB_HOST;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.ENV_INFLUXDB_TOKEN;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.ENV_INFLUXDB_TOKEN_KEY;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.MAX_EXCEPTION_CAUSE_SEARCH_DEPTH;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.MAX_TOKEN_REFRESH_RETRIES;
import static com.amazonaws.athena.connectors.influxdb.InfluxDBConstants.TOKEN_REFRESH_MAX_RETRIES;
/**
 * Creates InfluxDB client connections, resolving the auth token from Secrets Manager.
 *
 * Token resolution supports two formats: 1. Plain string secret — the entire secret value is the token. 2. JSON secret — a JSON object; the token is extracted
 * from a configurable key (env var influxdb_token_key, defaults to "token").
 *
 * The env var influxdb_token can be either a literal token or a Secrets Manager reference using the SDK's ${secret_name} pattern.
 */
public class InfluxDBConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDBConnectionFactory.class);
    private static final Gson GSON = new Gson();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    static final int INFLUXDB_CLIENT_CACHE_CAPACITY = 100;
    static final long BACKOFF_BASE_MILLIS = 100L;
    static final long BACKOFF_MAX_MILLIS = 1_000L;
    private static final int INFLUXDB_CLIENT_CACHE_MINUTES_TO_LIVE = 30;

    private volatile String resolvedToken;
    private final Cache<String, InfluxDBClient> influxDbClients;
    private final Map<String, String> configOptions;
    private final int maxTokenRefreshRetries;
    private FederationRequestHandler handler;

    public InfluxDBConnectionFactory(final Map<String, String> configOptions, final FederationRequestHandler handler)
    {
        this.configOptions = configOptions;
        this.handler = handler;
        this.influxDbClients = CacheBuilder.newBuilder()
            .maximumSize(INFLUXDB_CLIENT_CACHE_CAPACITY)
            .expireAfterAccess(Duration.ofMinutes(INFLUXDB_CLIENT_CACHE_MINUTES_TO_LIVE))
            .removalListener((final RemovalNotification<String, InfluxDBClient> notification) -> {
                final InfluxDBClient client = notification.getValue();
                if (client != null) {
                    try {
                        client.close();
                    }
                    catch (final Exception e) {
                        logger.warn("Failed to close evicted InfluxDBClient for db '{}'", notification.getKey(), e);
                    }
                }
            })
            .build();
        this.resolvedToken = null;
        this.maxTokenRefreshRetries = parseMaxTokenRefreshRetries(configOptions);
        // The Athena federation SDK exposes no explicit container-teardown callback, so release cached clients
        // (gRPC channels, threads, allocators) via a JVM shutdown hook when the Lambda environment is torn down.
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeAllClients, "influxdb-client-cache-shutdown"));
    }

    private static int parseMaxTokenRefreshRetries(final Map<String, String> configOptions)
    {
        final String configured = configOptions.get(TOKEN_REFRESH_MAX_RETRIES);
        if (configured == null || configured.isBlank()) {
            return DEFAULT_TOKEN_REFRESH_MAX_RETRIES;
        }
        try {
            // Clamped so a misconfiguration cannot turn one failing request into an unbounded burst.
            return Math.max(0, Math.min(Integer.parseInt(configured.trim()), MAX_TOKEN_REFRESH_RETRIES));
        }
        catch (final NumberFormatException e) {
            logger.warn("Invalid {} value '{}'; using default {}",
                    TOKEN_REFRESH_MAX_RETRIES, configured, DEFAULT_TOKEN_REFRESH_MAX_RETRIES);
            return DEFAULT_TOKEN_REFRESH_MAX_RETRIES;
        }
    }

    /**
     * Sets the handler reference for secret resolution. Used when the handler cannot be passed at construction time (e.g., RecordHandler passes itself after
     * super() completes).
     */
    public void setHandler(final FederationRequestHandler handler)
    {
        this.handler = handler;
    }

    /**
     * Validates the configured InfluxDB host and returns it with a canonical, lowercase scheme.
     *
     * The bearer token is sent on every request, so the host must use HTTPS. Validation parses the
     * URL instead of matching a string prefix, because the InfluxDB client enables TLS for Flight
     * only when the scheme is exactly {@code https}; any other value (for example {@code HTTPS://},
     * {@code grpc://}, or a host with no scheme) makes the client connect in plaintext. Returning the
     * lowercased scheme guarantees the client sees {@code https} whenever this check passes.
     *
     * Plain {@code http} is accepted only when {@code ALLOW_INSECURE_TRANSPORT=true}, which is
     * intended for local testing against a server without TLS.
     *
     * @throws IllegalArgumentException if the host is missing, is not an absolute URL, or does not
     *             use HTTPS (or HTTP with insecure transport explicitly allowed)
     */
    String validatedHost()
    {
        final String configured = configOptions.get(ENV_INFLUXDB_HOST);
        if (configured == null || configured.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_HOST);
        }
        final String host = configured.trim();
        final URI uri;
        try {
            uri = new URI(host);
        }
        catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Invalid host: '" + host + "'. Host must be a valid URL", e);
        }
        if (uri.getScheme() == null || uri.getHost() == null) {
            throw new IllegalArgumentException(
                "Invalid host: '" + host + "'. Host must be an absolute URL such as https://<endpoint>:8181");
        }
        final String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        final boolean allowInsecureTransport =
            Boolean.parseBoolean(configOptions.getOrDefault(ENV_ALLOW_INSECURE_TRANSPORT, "false"));
        if (!"https".equals(scheme) && !("http".equals(scheme) && allowInsecureTransport)) {
            throw new IllegalArgumentException("Invalid host: '" + host + "'. Host must use HTTPS");
        }
        return scheme + host.substring(uri.getScheme().length());
    }

    /**
     * Applies the {@code influxdb_database} access boundary and returns the canonical database name to
     * use when talking to InfluxDB.
     *
     * When the connector is scoped to a single database ({@code influxdb_database} is set), that
     * database is the only permissible target: the operator-configured, case-sensitive name is always
     * returned, and a request for any other database is rejected. Requests are matched
     * case-insensitively because Athena lowercases identifiers, but the returned name preserves the
     * configured case so a request differing only in case cannot be redirected to a distinct,
     * case-sensitive sibling database. When no scope is configured, the requested name is returned
     * unchanged (an empty request stays empty for the caller to handle).
     *
     * This is the single boundary check shared by {@link #getClient} and {@link #resolveDatabase};
     * {@code influxdb_database} is an enforced access boundary, not merely a discovery filter.
     *
     * @throws IllegalArgumentException if a database outside the configured scope is requested
     */
    private String enforceDatabaseScope(final String requested)
    {
        final String configuredDb = configOptions.getOrDefault("influxdb_database", "");
        final String req = (requested == null) ? "" : requested;
        if (configuredDb.isEmpty()) {
            return req;
        }
        if (req.isEmpty() || configuredDb.equalsIgnoreCase(req)) {
            return configuredDb;
        }
        throw new IllegalArgumentException(
            "Access to database '" + req + "' is denied; this connector is scoped to '" + configuredDb + "'");
    }

    /**
     * Creates an InfluxDBClient for the given database.
     *
     * @param database
     *            the database to connect to, or null to use the configured default
     */
    public InfluxDBClient getClient(final String database)
    {
        // Validate transport security before resolving the token so a misconfigured host fails
        // closed without the token ever being read or sent.
        final String host = validatedHost();

        final String token = resolveToken();

        final String db = enforceDatabaseScope(database);
        if (db.isEmpty()) {
            throw new IllegalArgumentException("No database specified and no influxdb_database default is configured");
        }

        final InfluxDBClient cachedInfluxDbClient = influxDbClients.getIfPresent(db);
        if (cachedInfluxDbClient != null) {
            return cachedInfluxDbClient;
        }
        assertDatabaseExists(db);
        try {
            return influxDbClients.get(db, () -> InfluxDBClient.getInstance(host, token.toCharArray(), db));
        }
        catch (final ExecutionException e) {
            // InfluxDBClient.getInstance throws only unchecked exceptions, so Guava never actually wraps a checked
            // one here; unwrap defensively rather than leak a checked exception that cannot occur.
            final Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException("Failed to create InfluxDB client for database '" + db + "'", cause);
        }
    }

    /**
    * Verifies {@code database} is an existing database on the target server, failing closed if it is not.
    * Gates {@link #getClient} so a caller-supplied name cannot mint (and cache) a live client for a database
    * that doesn't exist — this is what bounds the client cache to real databases.
    *
    * Expects an already case-resolved name (see {@link #resolveDatabase}); the match is exact because InfluxDB
    * database names are case-sensitive and the minted client will query with this exact name.
    */
    private void assertDatabaseExists(final String database)
    {
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("Database name must be provided");
        }
        final boolean exists;
        try {
            // Single request: this runs inside a retrying operation (a getClient cache miss under
            // withCredentialRefresh), so it must not start its own retry loop.
            exists = fetchDatabases().stream()
                .anyMatch(db -> db != null && database.equals(db.name));
        }
        catch (final IOException e) {
            throw new RuntimeException("Failed to verify database '" + database + "' exists", e);
        }
        catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while verifying database '" + database + "' exists", e);
        }
        if (!exists) {
            throw new IllegalArgumentException("Database does not exist: '" + database + "'");
        }
    }

    /**
     * A query against an {@link InfluxDBClient} that may fail with an auth error. The operation MUST fully materialize
     * or consume its results before returning, because InfluxDB's Flight streams are lazy — auth errors surface during
     * stream consumption, not when {@code query()} is called.
     */
    @FunctionalInterface
    public interface InfluxDBQuery<T>
    {
        T run(InfluxDBClient client) throws Exception;
    }

    /**
     * One attempt of an operation that may fail with an authentication error.
     */
    @FunctionalInterface
    interface Attempt<T>
    {
        T run() throws Exception;
    }

    /**
     * Runs {@code query} against a client for {@code database}, under {@link #withCredentialRefresh}.
     *
     * Client creation happens inside the attempt, so the database-existence check that a cache miss
     * triggers (see {@link #assertDatabaseExists}) shares this operation's single retry budget. It never
     * starts a second, nested retry loop.
     *
     * Auth errors occur at Flight stream initiation, before any rows are emitted, so retrying a query that streams into
     * a spiller does not risk duplicate output.
     */
    public <T> T executeWithTokenRetry(final String database, final InfluxDBQuery<T> query) throws Exception
    {
        return withCredentialRefresh(() -> query.run(getClient(database)));
    }

    /**
     * Runs {@code attempt}. If it fails with an authentication error (HTTP 401 or Flight
     * {@code UNAUTHENTICATED}; see {@link #isAuthError}), for example because the cached token was
     * rotated, invalidates the cached token and clients, waits a bounded exponential backoff, and
     * retries, up to {@link #maxTokenRefreshRetries} times. Every other failure, including an
     * authorization denial (HTTP 403 or Flight {@code UNAUTHORIZED}), propagates immediately with no
     * retry and no invalidation.
     *
     * This is the only retry loop in the connection factory. Operations inside an attempt must use
     * non-retrying calls (such as {@link #fetchDatabases}) so retry budgets never multiply.
     */
    <T> T withCredentialRefresh(final Attempt<T> attempt) throws Exception
    {
        int refreshes = 0;
        while (true) {
            try {
                return attempt.run();
            }
            catch (final Exception e) {
                if (isAuthError(e) && refreshes < maxTokenRefreshRetries) {
                    refreshes++;
                    logger.warn("Authentication error from InfluxDB; invalidating token and retrying (attempt {} of {})",
                            refreshes, maxTokenRefreshRetries);
                    invalidateToken();
                    Thread.sleep(backoffMillis(refreshes));
                    continue;
                }
                if (isThrottle(e) && !(e instanceof FederationThrottleException)) {
                    throw new FederationThrottleException("InfluxDB throttled the request", e);
                }
                throw e;
            }
        }
    }

    /**
     * Delay before the given retry (1-based): {@link #BACKOFF_BASE_MILLIS} doubled for each earlier
     * retry, capped at {@link #BACKOFF_MAX_MILLIS}.
     */
    static long backoffMillis(final int retry)
    {
        final int doublings = Math.max(0, Math.min(retry - 1, 20));
        return Math.min(BACKOFF_MAX_MILLIS, BACKOFF_BASE_MILLIS << doublings);
    }

    /**
     * True if the throwable (or anything in its cause chain) indicates the downstream is throttling us: a Flight
     * {@code RESOURCE_EXHAUSTED} or an HTTP 429.
     */
    static boolean isThrottle(final Throwable throwable)
    {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < MAX_EXCEPTION_CAUSE_SEARCH_DEPTH; cause = cause.getCause(), depth++) {
            if (cause instanceof FlightRuntimeException
                    && ((FlightRuntimeException) cause).status().code() == FlightStatusCode.RESOURCE_EXHAUSTED) {
                return true;
            }
            if (cause instanceof InfluxDBApiHttpException
                    && ((InfluxDBApiHttpException) cause).statusCode() == 429) {
                return true;
            }
        }
        return false;
    }

    /**
     * Clears the cached token and closes+evicts all cached clients so the next {@link #getClient} rebuilds with a
     * freshly resolved token. Called on an auth failure that may indicate a rotated secret.
     */
    synchronized void invalidateToken()
    {
        resolvedToken = null;
        influxDbClients.invalidateAll();
        influxDbClients.cleanUp();
    }

    /**
     * Closes and evicts every cached client, releasing each one's gRPC channel, threads, and allocator. Wired to a JVM
     * shutdown hook so cached clients are released on container teardown. Closing runs through the cache's removal
     * listener; {@code invalidateAll} + {@code cleanUp} guarantees the listener fires for every entry. Idempotent.
     */
    synchronized void closeAllClients()
    {
        influxDbClients.invalidateAll();
        influxDbClients.cleanUp();
    }

    /**
     * Number of live cached clients, after running pending evictions.
     */
    long cachedClientCount()
    {
        influxDbClients.cleanUp();
        return influxDbClients.size();
    }

    /**
     * True if the throwable (or anything in its cause chain) is an authentication failure, meaning the
     * token was rejected and a freshly resolved (possibly rotated) token may succeed: a Flight
     * {@code UNAUTHENTICATED} or an HTTP 401.
     *
     * Authorization denials are deliberately excluded: a Flight {@code UNAUTHORIZED} (gRPC
     * {@code PERMISSION_DENIED}) or an HTTP 403 means the token was accepted but lacks permission for the
     * request. Retrying with the same or a refreshed token returns the same denial, so these are
     * terminal and must not invalidate the token or the client cache.
     */
    static boolean isAuthError(final Throwable throwable)
    {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < MAX_EXCEPTION_CAUSE_SEARCH_DEPTH; cause = cause.getCause(), depth++) {
            if (cause instanceof FlightRuntimeException
                    && ((FlightRuntimeException) cause).status().code() == FlightStatusCode.UNAUTHENTICATED) {
                return true;
            }
            if (cause instanceof InfluxDBApiHttpException
                    && ((InfluxDBApiHttpException) cause).statusCode() == 401) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves a lowercased schema name back to the original database name. Athena lowercases all identifiers, but InfluxDB is case-sensitive.
     *
     * When scoped to a single database via {@code influxdb_database}, that database is the only
     * permissible target: any other schema is rejected outright (see {@link #enforceDatabaseScope})
     * rather than being resolved against the full set of token-reachable databases.
     *
     * Otherwise fails closed: if the schema is not among the databases discoverable on the server, throws
     * instead of returning the requested name unchanged. Returning the unverified name would let a
     * caller-supplied schema masquerade as a real database and defer a guaranteed failure to a later stage.
     *
     * @throws IllegalArgumentException if the schema is outside the configured scope or does not resolve to an existing database
     * @throws InterruptedException
     * @throws IOException
     */
    public String resolveDatabase(final String schemaName) throws IOException, InterruptedException
    {
        final String configuredDb = this.configOptions.getOrDefault("influxdb_database", "");
        if (!configuredDb.isEmpty()) {
            return enforceDatabaseScope(schemaName);
        }
        return listDatabases().stream().map(db -> db != null ? db.name : null)
                .filter(name -> name != null && name.equalsIgnoreCase(schemaName)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Database not found: '" + schemaName + "'"));
    }

    /**
     * Resolves a lowercased table name back to the original case by querying information_schema given an
     * already-resolved database. Throws if no matching table exists — there is no correct-case name to fall back to,
     * and proceeding with the lowercased name would only defer a guaranteed failure to the read stage.
     */
    public String resolveTableName(final String resolvedDB, final TableName tableName) throws Exception
    {
        final Map<String, Object> parameters = Map.of("table_name", tableName.getTableName().toLowerCase(Locale.ROOT));
        final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' AND lower(table_name) = $table_name";
        return executeWithTokenRetry(resolvedDB, client -> {
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                return stream.map(row -> String.valueOf(row[0]))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException(
                                "Table not found in database '" + resolvedDB + "': " + tableName.getTableName()));
            }
        });
    }

    /**
     * Lists the databases on the server. A top-level operation: an expired or rotated token (HTTP 401)
     * is refreshed and retried under {@link #withCredentialRefresh}; any other failure, including an
     * authorization denial (HTTP 403), propagates after a single request.
     */
    List<DatabaseInfo> listDatabases() throws IOException, InterruptedException
    {
        try {
            return withCredentialRefresh(this::fetchDatabases);
        }
        catch (final IOException | InterruptedException | RuntimeException e) {
            throw e;
        }
        catch (final Exception e) {
            // fetchDatabases throws only the exceptions handled above.
            throw new IllegalStateException(e);
        }
    }

    /**
     * Sends exactly one request to list the databases on the server, with no retry. Callers that run
     * inside a retrying operation (see {@link #assertDatabaseExists}) use this so their attempts count
     * against that operation's single retry budget instead of starting a nested retry loop.
     *
     * @throws InfluxDBApiHttpException for a non-200, non-429 status, so {@link #isAuthError} and the
     *             caller's retry logic can classify it by status code
     * @throws FederationThrottleException for HTTP 429
     */
    List<DatabaseInfo> fetchDatabases() throws IOException, InterruptedException
    {
        final String host = validatedHost();
        final String token = resolveToken();
        final HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(host).resolve("/api/v3/configure/database?format=json"))
                .timeout(Duration.ofMinutes(2))
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json")
                .GET()
                .build();
        final HttpResponse<String> httpResponse = HTTP.send(httpRequest, BodyHandlers.ofString());
        final int statusCode = httpResponse.statusCode();
        if (statusCode == 200) {
            final List<DatabaseInfo> parsedJson = GSON.fromJson(httpResponse.body(),
                    new TypeToken<List<DatabaseInfo>>() {
                    }.getType());
            return parsedJson != null ? parsedJson : List.of();
        }
        if (statusCode == 429) {
            throw new FederationThrottleException(
                    "InfluxDB throttled the request listing databases in host " + host);
        }
        throw new InfluxDBApiHttpException(
                "Failed to list databases in host " + host + ": status code: " + statusCode,
                httpResponse.headers(), statusCode);
    }

    /**
     * Resolves the InfluxDB auth token. Supports: 1. ${secret_name} pattern — resolved via Secrets Manager, then parsed as JSON or plain string 2. Literal
     * token value
     */
    String resolveToken()
    {
        if (this.resolvedToken != null) {
            return this.resolvedToken;
        }
        final String rawToken = configOptions.get(ENV_INFLUXDB_TOKEN);
        if (rawToken == null || rawToken.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_TOKEN);
        }

        // Use the SDK's built-in secret resolution for ${secret_name} patterns.
        final String resolved = handler.resolveSecrets(rawToken);

        // If the resolved value looks like JSON, extract the token key.
        String trimmed = resolved.trim();
        if (trimmed.startsWith("{")) {
            try {
                final JsonObject json = GSON.fromJson(trimmed, JsonObject.class);
                final String tokenKey = configOptions.getOrDefault(ENV_INFLUXDB_TOKEN_KEY, DEFAULT_TOKEN_KEY);
                if (json.has(tokenKey)) {
                    trimmed = json.get(tokenKey).getAsString();
                }
                else {
                    throw new ConfigurationException("JSON secret does not contain key '" + tokenKey + "'");
                }
            }
            catch (final JsonSyntaxException jse) {
                logger.warn("Failed to parse secret as JSON. Treating secret as a raw token");
            }
            catch (final Exception e) {
                throw new RuntimeException("Unexpected error occurred while parsing secret JSON: " + e.getMessage());
            }
        }
        this.resolvedToken = trimmed;
        return this.resolvedToken;
    }

    public static final class DatabaseInfo
    {
        @SerializedName("iox::database")
        String name;

        DatabaseInfo(final String name)
        {
            this.name = name;
        }
    }
}
