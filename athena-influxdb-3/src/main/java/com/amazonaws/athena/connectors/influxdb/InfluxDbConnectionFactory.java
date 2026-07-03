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
import com.influxdb.v3.client.InfluxDBClient;
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
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.DEFAULT_TOKEN_KEY;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_HOST;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_TOKEN;
import static com.amazonaws.athena.connectors.influxdb.InfluxDbConstants.ENV_INFLUXDB_TOKEN_KEY;

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

    private final Map<String, String> configOptions;
    private FederationRequestHandler handler;

    public InfluxDbConnectionFactory(final Map<String, String> configOptions, final FederationRequestHandler handler)
    {
        this.configOptions = configOptions;
        this.handler = handler;
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

        return InfluxDBClient.getInstance(host, token.toCharArray(), db);
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
        try (InfluxDBClient client = getClient(resolvedDb)) {
            final Map<String, Object> parameters = Map.of("table_name", tableName.getTableName().toLowerCase(Locale.ROOT));
            final String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' AND lower(table_name) = $table_name";
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                return stream.map(row -> String.valueOf(row[0]))
                        .findFirst()
                        .orElse(tableName.getTableName());
            }
        }
    }

    List<DatabaseInfo> listDatabases() throws IOException, InterruptedException
    {
        final String host = configOptions.get(ENV_INFLUXDB_HOST);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_HOST);
        }
        final String token = resolveToken();
        final HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(host + "/api/v3/configure/database?format=json"))
                .timeout(Duration.ofMinutes(2))
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json")
                .GET()
                .build();
        final HttpResponse<String> httpResponse = HTTP.send(httpRequest, BodyHandlers.ofString());
        if (httpResponse.statusCode() != 200) {
            throw new RuntimeException(
                    "Failed to list databases in host " + host + ": status code: " + httpResponse.statusCode());
        }
        final List<DatabaseInfo> parsedJson = GSON.fromJson(httpResponse.body(), new TypeToken<List<DatabaseInfo>>() {
        }.getType());
        return parsedJson != null ? parsedJson : List.of();
    }

    /**
     * Resolves the InfluxDB auth token. Supports: 1. ${secret_name} pattern — resolved via Secrets Manager, then parsed as JSON or plain string 2. Literal
     * token value
     */
    String resolveToken()
    {
        final String rawToken = configOptions.get(ENV_INFLUXDB_TOKEN);
        if (rawToken == null || rawToken.isEmpty()) {
            throw new IllegalArgumentException("Missing required env var: " + ENV_INFLUXDB_TOKEN);
        }

        // Use the SDK's built-in secret resolution for ${secret_name} patterns
        final String resolved = handler.resolveSecrets(rawToken);

        // If the resolved value looks like JSON, extract the token key
        final String trimmed = resolved.trim();
        if (trimmed.startsWith("{")) {
            try {
                final JsonObject json = GSON.fromJson(trimmed, JsonObject.class);
                final String tokenKey = configOptions.getOrDefault(ENV_INFLUXDB_TOKEN_KEY, DEFAULT_TOKEN_KEY);
                if (json.has(tokenKey)) {
                    return json.get(tokenKey).getAsString();
                }
                logger.warn("JSON secret does not contain key '{}', using raw value", tokenKey);
            }
            catch (final Exception e) {
                logger.warn("Failed to parse secret as JSON, using raw value");
            }
        }

        return resolved;
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
