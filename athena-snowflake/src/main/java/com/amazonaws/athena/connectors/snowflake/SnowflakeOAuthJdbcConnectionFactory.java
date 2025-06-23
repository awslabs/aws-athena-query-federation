/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.utils.Validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_CODE;

public class SnowflakeOAuthJdbcConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthJdbcConnectionFactory.class);
    public static final String ACCESS_TOKEN = "access_token";
    public static final String FETCHED_AT = "fetched_at";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String EXPIRES_IN = "expires_in";
    public static final String USERNAME = "username";
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;
    private final String oauthSecretName;
    private final Map<String, String> configOptions;
    private SecretsManagerClient secretsClient;

    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, properties, databaseConnectionInfo, configOptions, SecretsManagerClient.create());
    }

    @VisibleForTesting
    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, Map<String, String> configOptions,
                                               SecretsManagerClient secretsClient)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.configOptions = configOptions;
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
        this.secretsClient = secretsClient;
        this.oauthSecretName = Validate.notNull(databaseConnectionConfig.getSecret(), "Missing required property: secret name");
    }

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider)
    {
        try {
            final String derivedJdbcString;
            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(this.oauthSecretName)
                    .build();

            GetSecretValueResponse secretValue = this.secretsClient.getSecretValue(getSecretValueRequest);
            Map<String, String> oauthConfig = new ObjectMapper().readValue(secretValue.secretString(), Map.class);
            if (oauthConfig.containsKey(AUTH_CODE) && !oauthConfig.get(AUTH_CODE).isEmpty()) {
                Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(""));

                jdbcProperties.put("user", oauthConfig.get(USERNAME));
                // Get access token from OAuth provider using secret manager
                String accessToken = fetchAccessTokenFromSecret(oauthConfig);
                // Use the token in the connection properties
                jdbcProperties.put("password", accessToken);
                jdbcProperties.put("authenticator", "oauth");

                Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();

                return DriverManager.getConnection(derivedJdbcString, jdbcProperties);
            }
            else {
                    return super.getConnection(credentialsProvider);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error creating Snowflake connection: " + ex.getMessage(), ex);
        }
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        // Check if token related fields exist
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            return oauthConfig.get(ACCESS_TOKEN);
        }
        return null;
    }

    private void saveTokenToSecretsManager(JSONObject tokenJson, Map<String, String> oauthConfig)
    {
        // Update token related fields
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        oauthConfig.put(ACCESS_TOKEN, tokenJson.getString(ACCESS_TOKEN));
        oauthConfig.put(REFRESH_TOKEN, tokenJson.getString(REFRESH_TOKEN));
        oauthConfig.put(EXPIRES_IN, String.valueOf(tokenJson.getInt(EXPIRES_IN)));
        oauthConfig.put(FETCHED_AT, String.valueOf(tokenJson.getLong(FETCHED_AT)));

        // Save updated secret
        secretsClient.putSecretValue(builder -> builder
                .secretId(this.oauthSecretName)
                .secretString(String.valueOf(new JSONObject(oauthConfig)))
                .build());
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken;
        String clientId = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_ID), "Missing required property: client_id");
        String tokenEndpoint = Validate.notNull(oauthConfig.get(SnowflakeConstants.TOKEN_URL), "Missing required property: token_url");
        String redirectUri = Validate.notNull(oauthConfig.get(SnowflakeConstants.REDIRECT_URI), "Missing required property: redirect_uri");
        String clientSecret = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_SECRET), "Missing required property: client_secret");
        String authCode = Validate.notNull(oauthConfig.get(SnowflakeConstants.AUTH_CODE), "Missing required property: auth_code");

        accessToken = loadTokenFromSecretsManager(oauthConfig);

        if (accessToken == null) {
            LOGGER.debug("First time auth. Using authorization_code...");
            JSONObject tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            accessToken = tokenJson.getString(ACCESS_TOKEN);
        }
        else {
            long expiresIn = Long.parseLong(oauthConfig.get(EXPIRES_IN));
            long fetchedAt = Long.parseLong(oauthConfig.getOrDefault(FETCHED_AT, String.valueOf(0L)));
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid.");
            }
            else {
                LOGGER.debug("Access token expired. Using refresh_token...");
                JSONObject refreshed = refreshAccessToken(oauthConfig.get(REFRESH_TOKEN), tokenEndpoint, clientId, clientSecret);
                refreshed.put(REFRESH_TOKEN, oauthConfig.get(REFRESH_TOKEN));
                saveTokenToSecretsManager(refreshed, oauthConfig);
                accessToken = refreshed.getString(ACCESS_TOKEN);
            }
        }
        return accessToken;
    }

    private JSONObject getTokenFromAuthCode(String authCode, String redirectUri, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=authorization_code"
                + "&code=" + authCode
                + "&redirect_uri=" + redirectUri;

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private JSONObject refreshAccessToken(String refreshToken, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=refresh_token"
                + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);

       return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private JSONObject requestToken(String requestBody, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        HttpURLConnection conn = getHttpURLConnection(tokenEndpoint, clientId, clientSecret);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) ?
                conn.getInputStream() : conn.getErrorStream();

        String response = new BufferedReader(new InputStreamReader(is))
                .lines()
                .reduce("", (acc, line) -> acc + line);

        if (responseCode != 200) {
            throw new RuntimeException("Failed: " + responseCode + " - " + response);
        }

        JSONObject tokenJson = new JSONObject(response);
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        return tokenJson;
    }

    static HttpURLConnection getHttpURLConnection(String tokenEndpoint, String clientId, String clientSecret) throws IOException
    {
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        String authHeader = Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + authHeader);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        return conn;
    }

    public String getS3ExportBucket()
    {
        return configOptions.get("spill_bucket");
    }
}
