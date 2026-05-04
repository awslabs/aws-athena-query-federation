/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.OAuthCredentialsProvider;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.utils.Validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType.OAUTH;

/**
 * OAuth-only credentials provider for Snowflake in the Athena federation connector.
 * Snowflake OAuth uses the shared {@link OAuthCredentialsProvider} integration point so
 * {@link com.amazonaws.athena.connector.credentials.CredentialsProviderFactory} can treat Snowflake like other JDBC
 * connectors that support OAuth. Snowflake uses authorization_code and refresh_token grants with HTTP Basic client
 * authentication, so {@link #getCredential()} is overridden; {@link #buildTokenRequest} is unused.
 */
public class SnowflakeOAuthCredentialsProvider extends OAuthCredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthCredentialsProvider.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String REFRESH_TOKEN = "refresh_token";

    private String initializedSecretName;
    private Map<String, String> initializedSecretMap;
    private CachableSecretsManager initializedSecretsManager;

    public SnowflakeOAuthCredentialsProvider()
    {
        super();
    }

    @Override
    public void initialize(String secretName, Map<String, String> secretMap, CachableSecretsManager secretsManager)
    {
        super.initialize(secretName, secretMap, secretsManager);
        this.initializedSecretName = secretName;
        this.initializedSecretMap = secretMap;
        this.initializedSecretsManager = secretsManager;
    }

    @Override
    protected boolean isOAuthConfigured(Map<String, String> secretMap)
    {
        if (secretMap == null || secretMap.isEmpty()) {
            return false;
        }
        boolean hasUsername = StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.SF_USER))
                || StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.USERNAME));
        return StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.AUTH_CODE))
                && StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.REDIRECT_URI))
                && StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.CLIENT_SECRET))
                && StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.TOKEN_URL))
                && StringUtils.isNotBlank(secretMap.get(SnowflakeConstants.CLIENT_ID))
                && hasUsername;
    }

    /**
     * Satisfies the {@link OAuthCredentialsProvider} contract; Snowflake does not use the base class token path
     * ({@link java.net.http.HttpClient} plus a single built {@link HttpRequest}).
     * <p>
     * Snowflake OAuth exchanges authorization codes and refresh tokens over {@link HttpURLConnection} with HTTP Basic
     * client authentication in {@link #getCredential()} / {@code fetchAccessTokenFromSecret}, so this method is never
     * invoked for normal operation. If called, it fails fast rather than building an incorrect request.
     *
     * @param secretMap secret fields (unused; Snowflake token exchange does not use this hook)
     * @return never returns normally
     * @throws UnsupportedOperationException always; Snowflake OAuth is not wired to the default OAuth request builder
     */
    @Override
    protected HttpRequest buildTokenRequest(Map<String, String> secretMap)
    {
        throw new UnsupportedOperationException("Snowflake OAuth uses authorization_code / refresh_token exchange, not the default OAuthCredentialsProvider token path");
    }

    @Override
    public Credentials getCredential()
    {
        try {
            Map<String, String> oauthConfig = initializedSecretMap;
            String accessToken = fetchAccessTokenFromSecret(oauthConfig);
            String username = SnowflakeAuthUtils.getUsername(oauthConfig);
            return new SnowflakeOAuthJdbcCredentials(username, accessToken);
        }
        catch (AthenaConnectorException e) {
            throw e;
        }
        catch (Exception ex) {
            throw new AthenaConnectorException("Error retrieving Snowflake credentials: " + ex.getMessage(), ex,
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                            .errorMessage(ex.getMessage())
                            .build());
        }
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken;
        String clientId = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_ID), "Missing required property: client_id");
        String tokenEndpoint = Validate.notNull(oauthConfig.get(SnowflakeConstants.TOKEN_URL), "Missing required property: token_url");
        String redirectUri = Validate.notNull(oauthConfig.get(SnowflakeConstants.REDIRECT_URI), "Missing required property: redirect_uri");
        String clientSecret = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_SECRET), "Missing required property: client_secret");
        String authCode = Validate.notNull(oauthConfig.get(SnowflakeConstants.AUTH_CODE), "Missing required property: auth_code");

        accessToken = loadTokenFromSecret(oauthConfig);

        if (accessToken == null) {
            LOGGER.debug("First time auth. Using authorization_code...");
            ObjectNode tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            accessToken = tokenJson.get(CredentialsConstants.ACCESS_TOKEN).asText();
        }
        else {
            long expiresIn = Long.parseLong(oauthConfig.get(CredentialsConstants.EXPIRES_IN));
            long fetchedAt = Long.parseLong(oauthConfig.getOrDefault(CredentialsConstants.FETCHED_AT, String.valueOf(0L)));
            long now = System.currentTimeMillis() / 1000;

            if ((now - fetchedAt) < expiresIn - 60) {
                LOGGER.debug("Access token still valid.");
            }
            else {
                LOGGER.debug("Access token expired. Using refresh_token...");
                ObjectNode refreshed = refreshAccessToken(oauthConfig.get(REFRESH_TOKEN), tokenEndpoint, clientId, clientSecret);
                refreshed.put(REFRESH_TOKEN, oauthConfig.get(REFRESH_TOKEN));
                saveTokenToSecretsManager(refreshed, oauthConfig);
                accessToken = refreshed.get(CredentialsConstants.ACCESS_TOKEN).asText();
            }
        }
        return accessToken;
    }

    private String loadTokenFromSecret(Map<String, String> oauthConfig)
    {
        String accessToken = oauthConfig.get(CredentialsConstants.ACCESS_TOKEN);
        if (StringUtils.isNotBlank(accessToken)) {
            return accessToken;
        }
        return null;
    }

    private void saveTokenToSecretsManager(ObjectNode tokenJson, Map<String, String> oauthConfig)
    {
        tokenJson.put(CredentialsConstants.FETCHED_AT, System.currentTimeMillis() / 1000);
        oauthConfig.put(CredentialsConstants.ACCESS_TOKEN, tokenJson.get(CredentialsConstants.ACCESS_TOKEN).asText());
        oauthConfig.put(REFRESH_TOKEN, tokenJson.get(REFRESH_TOKEN).asText());
        oauthConfig.put(CredentialsConstants.EXPIRES_IN, String.valueOf(tokenJson.get(CredentialsConstants.EXPIRES_IN).asInt()));
        oauthConfig.put(CredentialsConstants.FETCHED_AT, String.valueOf(tokenJson.get(CredentialsConstants.FETCHED_AT).asLong()));

        try {
            String updatedSecretString = OBJECT_MAPPER.writeValueAsString(oauthConfig);
            initializedSecretsManager.getSecretsManager().putSecretValue(builder -> builder
                    .secretId(initializedSecretName)
                    .secretString(updatedSecretString)
                    .build());
        }
        catch (Exception e) {
            LOGGER.error("Failed to save updated secret: ", e);
            throw new AthenaConnectorException("Failed to save updated secret: " + e.getMessage(), e,
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                            .errorMessage(e.getMessage())
                            .build());
        }
    }

    private ObjectNode getTokenFromAuthCode(String authCode, String redirectUri, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=authorization_code"
                + "&code=" + authCode
                + "&redirect_uri=" + redirectUri;

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private ObjectNode refreshAccessToken(String refreshToken, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=refresh_token"
                + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);

        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }

    private ObjectNode requestToken(String requestBody, String tokenEndpoint, String clientId, String clientSecret) throws Exception
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
            LOGGER.error("OAuth token request failed with status: {} - {}", responseCode, response);
            throw new AthenaConnectorException("OAuth authentication failed with status: " + responseCode, ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_RESPONSE_EXCEPTION.toString()).build());
        }

        ObjectNode tokenJson = OBJECT_MAPPER.readValue(response, ObjectNode.class);
        tokenJson.put(CredentialsConstants.FETCHED_AT, System.currentTimeMillis() / 1000);
        return tokenJson;
    }

    @VisibleForTesting
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

    private static final class SnowflakeOAuthJdbcCredentials implements Credentials
    {
        private final String user;
        private final String accessToken;

        SnowflakeOAuthJdbcCredentials(String user, String accessToken)
        {
            this.user = user;
            this.accessToken = accessToken;
        }

        @Override
        public Map<String, String> getProperties()
        {
            Map<String, String> m = new HashMap<>();
            m.put(SnowflakeConstants.USER, user);
            m.put(SnowflakeConstants.PASSWORD, accessToken);
            m.put(SnowflakeConstants.AUTHENTICATOR, OAUTH.getValue());
            return Collections.unmodifiableMap(m);
        }
    }
}
