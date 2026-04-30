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
package com.amazonaws.athena.connectors.snowflake.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTHENTICATOR;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.PRIVATE_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.USER;
import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType.SNOWFLAKE_JWT;

/**
 * Custom connection factory for Snowflake that supports multiple authentication methods.
 * <p>
 * For JWT (key-pair) auth: uses {@link DriverManager} directly because HikariCP
 * stringifies all property values via {@code toString()}, which breaks the
 * {@link java.security.PrivateKey} object required by the Snowflake JDBC driver.
 * <p>
 * For password/OAuth auth: always uses HikariCP connection pooling via
 * {@link GenericJdbcConnectionFactory#getConnectionFromManagedPool}, bypassing
 * the parent's direct-connection path even when {@code fas_token} is present.
 * <p>
 * The Snowflake JDBC driver's {@code SnowflakeConnectString.parse()} URL-decodes
 * query parameters internally, so both {@code %22} and literal {@code "} in the
 * connection string work correctly for case-sensitive identifiers.
 */
public class SnowflakeConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectionFactory.class);

    public SnowflakeConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> properties, DatabaseConnectionInfo databaseConnectionInfo)
    {
        this(databaseConnectionConfig, properties, databaseConnectionInfo, null);
    }

    public SnowflakeConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> properties, DatabaseConnectionInfo databaseConnectionInfo, final Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo, configOptions);
    }

    @Override
    public Connection getConnection(CredentialsProvider credentialsProvider) throws Exception
    {
        if (credentialsProvider == null) {
            return super.getConnectionFromManagedPool(null);
        }
        else if (SNOWFLAKE_JWT.equals(SnowflakeAuthUtils.determineAuthType(credentialsProvider.getCredentialMap()))) {
            // Key-pair auth requires a PrivateKey object in Properties,
            // which HikariCP cannot handle (it stringifies all property values).
            // Use DriverManager directly with URL-decoded connection string.
            return getKeyPairConnection(credentialsProvider.getCredentialMap());
        }
        else {
            // Password and OAuth: always use HikariCP for connection pooling,
            // bypassing the parent's direct-connection path even when fas_token is present.
            return super.getConnectionFromManagedPool(credentialsProvider);
        }
    }

    /**
     * Creates a connection using key-pair authentication via {@link DriverManager}.
     * <p>
     * Secret placeholders are stripped and the connection string is URL-decoded as a
     * safety measure, though the Snowflake JDBC driver also decodes query parameters
     * internally via {@code SnowflakeConnectString.parse()}.
     */
    private Connection getKeyPairConnection(Map<String, String> credentialMap) throws SQLException
    {
        try {
            String username = credentialMap.get(USER);
            String privateKeyPem = credentialMap.get(SnowflakeConstants.PEM_PRIVATE_KEY);
            String passphrase = credentialMap.get(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE);

            java.security.PrivateKey privateKey = SnowflakeAuthUtils.createPrivateKey(privateKeyPem, passphrase);

            String jdbcString = databaseConnectionConfig.getJdbcConnectionString();
            Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcString);
            String cleanedString = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            String connectionString = URLDecoder.decode(cleanedString, StandardCharsets.UTF_8);

            Properties props = new Properties();
            props.putAll(jdbcProperties);
            props.setProperty(USER, username);
            props.setProperty(AUTHENTICATOR, SNOWFLAKE_JWT.getValue());
            props.put(PRIVATE_KEY, privateKey);

            LOGGER.debug("Creating Snowflake connection with key-pair authentication for user: {}", username);

            // Explicitly register the Snowflake JDBC driver with DriverManager.
            // The Athena Federation code path (FederationRequest) does not use HikariCP here,
            // so the driver may not be auto-registered. HikariCP handles this via its own
            // fallback mechanism, but DriverManager.getConnection() does not.
            Class.forName(databaseConnectionInfo.getDriverClassName());

            return DriverManager.getConnection(connectionString, props);
        }
        catch (Exception e) {
            LOGGER.error("Failed to create Snowflake connection with key-pair authentication", e);
            throw new SQLException("Failed to create Snowflake connection with key-pair authentication: " + e.getMessage(), e);
        }
    }
}
