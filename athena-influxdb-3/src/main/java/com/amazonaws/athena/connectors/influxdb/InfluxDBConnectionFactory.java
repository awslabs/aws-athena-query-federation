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
    private static final int INFLUXDB_CLIENT_CACHE_CAPACITY = 100;
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
            return Math.max(0, Integer.parseInt(configured.trim()));
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
        final String host = configOptions.get(ENV_INFLUXDB_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_HOST);
        }

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
            exists = listDatabases().stream()
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
     * Runs {@code query} against a client for {@code database}. If it fails with an auth error (e.g., the cached token
     * was rotated out from under us), invalidates the cached token + clients, rebuilds with a freshly resolved token,
     * and retries — up to {@link #maxTokenRefreshRetries} times. Non-auth errors, and auth errors past the retry cap
     * (e.g., a genuinely invalid token), propagate.
     *
     * Auth errors occur at Flight stream initiation, before any rows are emitted, so retrying a query that streams into
     * a spiller does not risk duplicate output.
     */
    public <T> T executeWithTokenRetry(final String database, final InfluxDBQuery<T> query) throws Exception
    {
        int refreshes = 0;
        while (true) {
            final InfluxDBClient client = getClient(database);
            try {
                return query.run(client);
            }
            catch (final Exception e) {
                if (isAuthError(e) && refreshes < maxTokenRefreshRetries) {
                    refreshes++;
                    logger.warn("Auth error from InfluxDB; invalidating token and retrying (attempt {} of {})",
                            refreshes, maxTokenRefreshRetries);
                    invalidateToken();
                    continue;
                }
                if (isThrottle(e)) {
                    throw new FederationThrottleException("InfluxDB throttled the request", e);
                }
                throw e;
            }
        }
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
     * True if the throwable (or anything in its cause chain) is an InfluxDB auth failure: a Flight
     * {@code UNAUTHENTICATED}/{@code UNAUTHORIZED} (raised by {@code query}/{@code queryBatches}) or an HTTP 401/403.
     */
    static boolean isAuthError(final Throwable throwable)
    {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < MAX_EXCEPTION_CAUSE_SEARCH_DEPTH; cause = cause.getCause(), depth++) {
            if (cause instanceof FlightRuntimeException) {
                final FlightStatusCode code = ((FlightRuntimeException) cause).status().code();
                if (code == FlightStatusCode.UNAUTHENTICATED || code == FlightStatusCode.UNAUTHORIZED) {
                    return true;
                }
            }
            if (cause instanceof InfluxDBApiHttpException) {
                final int statusCode = ((InfluxDBApiHttpException) cause).statusCode();
                if (statusCode == 401 || statusCode == 403) {
                    return true;
                }
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

    List<DatabaseInfo> listDatabases() throws IOException, InterruptedException
    {
        final String host = configOptions.get(ENV_INFLUXDB_HOST);
        final boolean allowInsecureTransport = Boolean.parseBoolean(configOptions.getOrDefault(ENV_ALLOW_INSECURE_TRANSPORT, "false"));
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_HOST);
        }
        if (host.startsWith("http://") && !allowInsecureTransport) {
            throw new IllegalArgumentException("Invalid host: '" + host + "'. Host must use HTTPS");
        }
        int refreshes = 0;
        while (true) {
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
            // On an auth failure, invalidate the (possibly rotated) token and retry, up to the cap.
            if ((statusCode == 401 || statusCode == 403) && refreshes < maxTokenRefreshRetries) {
                refreshes++;
                logger.warn("Auth error ({}) listing databases; invalidating token and retrying (attempt {} of {})",
                        statusCode, refreshes, maxTokenRefreshRetries);
                invalidateToken();
                continue;
            }
            if (statusCode == 429) {
                throw new FederationThrottleException(
                        "InfluxDB throttled the request listing databases in host " + host);
            }
            throw new RuntimeException(
                    "Failed to list databases in host " + host + ": status code: " + statusCode);
        }
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
