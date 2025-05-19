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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
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

    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;
    private final String oauthSecretName;
    private S3Client s3Client;
    private static final String TOKEN_OBJECT_KEY = "snowflake/oauth_token.json";
    private final Map<String, String> configOptions;

    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.configOptions = configOptions;
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
        this.s3Client = S3Client.create();
        this.oauthSecretName = Validate.notNull(databaseConnectionConfig.getSecret(), "Missing required property: secret name");
    }

    @VisibleForTesting
    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, Map<String, String> configOptions,
                                               S3Client s3Client)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.configOptions = configOptions;
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
        this.s3Client = s3Client;
        this.oauthSecretName = Validate.notNull(databaseConnectionConfig.getSecret(), "Missing required property: secret name");
    }

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider)
    {
        try {
            final String derivedJdbcString;
            SecretsManagerClient secretsClient = SecretsManagerClient.create();
            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(this.oauthSecretName)
                    .build();

            GetSecretValueResponse secretValue = secretsClient.getSecretValue(getSecretValueRequest);
            Map<String, String> oauthConfig = new ObjectMapper().readValue(secretValue.secretString(), Map.class);
            if (oauthConfig.containsKey(AUTH_CODE) && !oauthConfig.get(AUTH_CODE).isEmpty()) {
                if (credentialsProvider != null) {
                    Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                    derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(""));

                    jdbcProperties.put("user", credentialsProvider.getCredential().getUser());
                    // Get access token from OAuth provider using secret manager
                    String accessToken = fetchAccessTokenFromSecret(oauthConfig);
                    // Use the token in the connection properties
                    jdbcProperties.put("password", accessToken);
                    jdbcProperties.put("authenticator", "oauth");

                    Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();

                    return DriverManager.getConnection(derivedJdbcString, jdbcProperties);
                }
            }
            else {
                    return super.getConnection(credentialsProvider);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Error creating Snowflake connection: " + ex.getMessage(), ex);
        }
        return null;
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String clientId = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_ID), "Missing required property: client_id");
        String tokenEndpoint = Validate.notNull(oauthConfig.get(SnowflakeConstants.TOKEN_URL), "Missing required property: token_url");
        String redirectUri = Validate.notNull(oauthConfig.get(SnowflakeConstants.REDIRECT_URI), "Missing required property: redirect_uri");
        String clientSecret = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_SECRET), "Missing required property: client_secret");
        String authCode = Validate.notNull(oauthConfig.get(SnowflakeConstants.AUTH_CODE), "Missing required property: auth_code");

        JSONObject tokenJson = loadTokenJsonFromS3();

        if (tokenJson == null) {
            LOGGER.debug("First time auth. Using authorization_code...");
            tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenJsonToS3(tokenJson); // this will include refresh_token
        }
        else {
            long expiresIn = tokenJson.getLong("expires_in");
            long fetchedAt = tokenJson.optLong("fetched_at", 0);
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid.");
            }
            else {
                LOGGER.debug("Access token expired. Using refresh_token...");
                JSONObject refreshed = refreshAccessToken(tokenJson.getString("refresh_token"), tokenEndpoint, clientId, clientSecret);

                // Only update selected fields in the existing tokenJson
                tokenJson.put("access_token", refreshed.getString("access_token"));
                tokenJson.put("expires_in", refreshed.getInt("expires_in"));
                tokenJson.put("fetched_at", refreshed.getLong("fetched_at"));

                saveTokenJsonToS3(tokenJson);
            }
        }
        return tokenJson.getString("access_token");
    }

    private JSONObject loadTokenJsonFromS3()
    {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(getS3ExportBucket())
                    .key(TOKEN_OBJECT_KEY)
                    .build();

            InputStream is = s3Client.getObject(request);
            String content = new BufferedReader(new InputStreamReader(is))
                    .lines()
                    .reduce("", (acc, line) -> acc + line);
            return new JSONObject(content);
        }
        catch (NoSuchKeyException e) {
            LOGGER.error("Token file not found in S3.", e);
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException("Error reading token from S3: " + e.getMessage(), e);
        }
    }

    private void saveTokenJsonToS3(JSONObject json)
    {
        try {
            json.put("fetched_at", System.currentTimeMillis() / 1000);
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(getS3ExportBucket())
                    .key(TOKEN_OBJECT_KEY)
                    .contentType("application/json")
                    .build();
            s3Client.putObject(request, RequestBody.fromString(json.toString()));
        }
        catch (Exception e) {
            throw new RuntimeException("Error writing token to S3: " + e.getMessage(), e);
        }
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
        tokenJson.put("fetched_at", System.currentTimeMillis() / 1000);
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
