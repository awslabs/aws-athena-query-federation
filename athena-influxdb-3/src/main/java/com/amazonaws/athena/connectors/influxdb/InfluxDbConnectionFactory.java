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
import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.DEFAULT_TOKEN_KEY;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.DEFAULT_TOKEN_REFRESH_MAX_RETRIES;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_HOST;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_TOKEN;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_TOKEN_KEY;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.TOKEN_REFRESH_MAX_RETRIES;

/**
 * Creates InfluxDB client connections, resolving the auth token from Secrets Manager.
 *
 * Token resolution supports two formats: 1. Plain string secret — the entire secret value is the token. 2. JSON secret — a JSON object; the token is extracted
 * from a configurable key (env var influxdb_token_key, defaults to "token").
 *
 * The env var influxdb_token can be either a literal token or a Secrets Manager reference using the SDK's ${secret_name} pattern.
 */
public class InfluxDbConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(InfluxDbConnectionFactory.class);
    private static final Gson GSON = new Gson();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private volatile String resolvedToken;
    private final Map<String, InfluxDBClient> influxDbClients;
    private final Map<String, String> configOptions;
    private final int maxTokenRefreshRetries;
    private FederationRequestHandler handler;

    public InfluxDbConnectionFactory(final Map<String, String> configOptions, final FederationRequestHandler handler)
    {
        this.configOptions = configOptions;
        this.handler = handler;
        this.influxDbClients = new ConcurrentHashMap<>();
        this.resolvedToken = null;
        this.maxTokenRefreshRetries = parseMaxTokenRefreshRetries(configOptions);
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
        String db = database;
        if (db == null || db.isEmpty()) {
            db = configOptions.getOrDefault("influxdb_database", "");
        }

        return influxDbClients.computeIfAbsent(db, key -> InfluxDBClient.getInstance(host, token.toCharArray(), key));
    }

    /**
     * A query against an {@link InfluxDBClient} that may fail with an auth error. The operation MUST fully materialize
     * or consume its results before returning, because InfluxDB's Flight streams are lazy — auth errors surface during
     * stream consumption, not when {@code query()} is called.
     */
    @FunctionalInterface
    public interface InfluxDbQuery<T>
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
    public <T> T executeWithTokenRetry(final String database, final InfluxDbQuery<T> query) throws Exception
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
                throw e;
            }
        }
    }

    /**
     * Clears the cached token and closes+evicts all cached clients so the next {@link #getClient} rebuilds with a
     * freshly resolved token. Called on an auth failure that may indicate a rotated secret.
     */
    synchronized void invalidateToken()
    {
        resolvedToken = null;
        for (final InfluxDBClient client : influxDbClients.values()) {
            try {
                client.close();
            }
            catch (final Exception e) {
                logger.warn("Failed to close cached InfluxDBClient during token invalidation", e);
            }
        }
        influxDbClients.clear();
    }

    /**
     * True if the throwable (or anything in its cause chain) is an InfluxDB auth failure: a Flight
     * {@code UNAUTHENTICATED}/{@code UNAUTHORIZED} (raised by {@code query}/{@code queryBatches}) or an HTTP 401/403.
     */
    static boolean isAuthError(final Throwable throwable)
    {
        Throwable cause = throwable;
        for (int depth = 0; cause != null && depth < 32; cause = cause.getCause(), depth++) {
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
     * @throws InterruptedException
     * @throws IOException
     */
    public String resolveDatabase(final String schemaName) throws IOException, InterruptedException
    {
        final String configuredDb = this.configOptions.getOrDefault("influxdb_database", "");
        if (!configuredDb.isEmpty() && configuredDb.equalsIgnoreCase(schemaName)) {
            return configuredDb;
        }
        return listDatabases().stream().map(db -> db != null ? db.name : null)
                .filter(name -> name != null && name.equalsIgnoreCase(schemaName)).findFirst().orElse(schemaName);
    }

    /**
     * Resolves a lowercased table name back to the original case by querying information_schema given an already-resolved database.
     */
    public String resolveTableName(final String resolvedDb, final TableName tableName) throws Exception
    {
        final Map<String, Object> parameters = Map.of("table_name", tableName.getTableName().toLowerCase(Locale.ROOT));
        final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' AND lower(table_name) = $table_name";
        return executeWithTokenRetry(resolvedDb, client -> {
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                return stream.map(row -> String.valueOf(row[0]))
                        .findFirst()
                        .orElse(tableName.getTableName());
            }
        });
    }

    List<DatabaseInfo> listDatabases() throws IOException, InterruptedException
    {
        final String host = configOptions.get(ENV_INFLUXDB_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_HOST);
        }
        int refreshes = 0;
        while (true) {
            final String token = resolveToken();
            final HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(host + "/api/v3/configure/database?format=json"))
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
        if (resolvedToken != null) {
            return resolvedToken;
        }
        final String rawToken = configOptions.get(ENV_INFLUXDB_TOKEN);
        if (rawToken == null || rawToken.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_TOKEN);
        }

        // Use the SDK's built-in secret resolution for ${secret_name} patterns
        final String resolved = handler.resolveSecrets(rawToken);

        // If the resolved value looks like JSON, extract the token key
        final String trimmed = resolved.trim();
        resolvedToken = trimmed;
        if (resolvedToken.startsWith("{")) {
            try {
                final JsonObject json = GSON.fromJson(trimmed, JsonObject.class);
                final String tokenKey = configOptions.getOrDefault(ENV_INFLUXDB_TOKEN_KEY, DEFAULT_TOKEN_KEY);
                if (json.has(tokenKey)) {
                    resolvedToken = json.get(tokenKey).getAsString();
                }
                else {
                    logger.warn("JSON secret does not contain key '{}', using raw value", tokenKey);
                }
            }
            catch (final Exception e) {
                logger.warn("Failed to parse secret as JSON, using raw value");
            }
        }
        return resolvedToken;
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
