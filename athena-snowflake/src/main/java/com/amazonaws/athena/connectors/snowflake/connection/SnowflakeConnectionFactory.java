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
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthType;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Custom connection factory for Snowflake that supports key-pair authentication.
 * Extends GenericJdbcConnectionFactory to handle Snowflake-specific authentication methods.
 */
public class SnowflakeConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectionFactory.class);
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;

    /**
     * @param databaseConnectionConfig database connection configuration {@link DatabaseConnectionConfig}
     * @param properties JDBC connection properties.
     * @param databaseConnectionInfo Contains JDBC driver and default port details.
     */
    public SnowflakeConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> properties, DatabaseConnectionInfo databaseConnectionInfo)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
    }

    @Override
    public Connection getConnection(CredentialsProvider credentialsProvider) throws Exception
    {
        if (credentialsProvider == null) {
            // Fall back to parent implementation for no credentials
            return super.getConnection(null);
        }

        Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
        SnowflakeAuthType authType = SnowflakeAuthUtils.determineAuthType(credentialMap);

        if (SNOWFLAKE_JWT.equals(authType)) {
            // Handle key-pair authentication
            return getKeyPairConnection(credentialMap);
        }
        else {
            // Use parent implementation for password and OAuth authentication
            return super.getConnection(credentialsProvider);
        }
    }

    /**
     * Creates a connection using key-pair authentication.
     * 
     * @param credentialMap The credential map containing username and private key
     * @return JDBC connection
     * @throws SQLException if connection fails
     */
    private Connection getKeyPairConnection(Map<String, String> credentialMap) throws SQLException
    {
        try {
            String username = credentialMap.get(USER);
            String privateKeyPem = credentialMap.get(SnowflakeConstants.PEM_PRIVATE_KEY);
            String passphrase =  credentialMap.get(SnowflakeConstants.PEM_PRIVATE_KEY_PASSPHRASE);
            
            // Create private key object
            java.security.PrivateKey privateKey = SnowflakeAuthUtils.createPrivateKey(privateKeyPem, passphrase);
            
            // Build connection string
            String connectionString = buildConnectionString();
            
            // Create properties for key-pair authentication
            Properties props = new Properties();
            props.putAll(jdbcProperties);
            props.setProperty(USER, username);
            props.setProperty(AUTHENTICATOR, SNOWFLAKE_JWT.getValue());
            props.put(PRIVATE_KEY, privateKey);
            
            LOGGER.debug("Creating Snowflake connection with key-pair authentication for user: {}", username);
            
            return DriverManager.getConnection(connectionString, props);
        }
        catch (Exception e) {
            LOGGER.error("Failed to create Snowflake connection with key-pair authentication", e);
            throw new SQLException("Failed to create Snowflake connection with key-pair authentication: " + e.getMessage(), e);
        }
    }

    /**
     * Builds the JDBC connection string for Snowflake.
     * 
     * @return The connection string
     */
    private String buildConnectionString()
    {
        String jdbcString = databaseConnectionConfig.getJdbcConnectionString();
        
        // Remove any secret placeholders from the connection string
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcString);
        return secretMatcher.replaceAll(Matcher.quoteReplacement(""));
    }
}
