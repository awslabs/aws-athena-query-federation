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
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils.getSecretName;

/**
 * Extends the GenericJdbcConnectionFactory to support private key authentication for Snowflake.
 */
public class SnowflakeConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectionFactory.class);
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Map<String, String> properties;

    /**
     * Creates a new SnowflakeConnectionFactory.
     *
     * @param databaseConnectionConfig Database connection configuration
     * @param properties JDBC connection properties
     * @param databaseConnectionInfo Database connection information
     */
    public SnowflakeConnectionFactory(
            DatabaseConnectionConfig databaseConnectionConfig,
            Map<String, String> properties,
            DatabaseConnectionInfo databaseConnectionInfo)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionConfig = databaseConnectionConfig;
        this.properties = properties;
        this.databaseConnectionInfo = databaseConnectionInfo;
    }

    @Override
    public Connection getConnection(CredentialsProvider credentialsProvider)
        throws SQLException
    {
        try {
            LOGGER.debug("getConnection called with credentialsProvider: {}",
                    credentialsProvider != null ? credentialsProvider.getClass().getName() : "null");

            if (credentialsProvider == null) {
                LOGGER.error("CredentialsProvider is null");
                throw new SQLException("CredentialsProvider is null");
            }

            // Check if credentialsProvider is SnowflakePrivateKeyCredentialProvider
            boolean isPrivateKeyAuth = credentialsProvider instanceof SnowflakePrivateKeyCredentialProvider;

            // For private key authentication, we need to use our own implementation
            if (isPrivateKeyAuth) {
                LOGGER.info("Using private key authentication");
                SnowflakePrivateKeyCredentialProvider privateKeyProvider =
                        (SnowflakePrivateKeyCredentialProvider) credentialsProvider;

                // Process the connection string
                String processedConnectionString = processConnectionString(databaseConnectionConfig.getJdbcConnectionString());

                // Create connection properties
                Properties connectionProps = new Properties();
                if (properties != null) {
                    connectionProps.putAll(properties);
                }
                connectionProps.put("user", privateKeyProvider.getCredential().getUser());

                try {
                    // Set PrivateKey object instead of string
                    PrivateKey privateKeyObj = privateKeyProvider.getPrivateKeyObject();
                    connectionProps.put("privateKey", privateKeyObj);
                    LOGGER.debug("Private key object created successfully");
                }
                catch (Exception e) {
                    LOGGER.error("Failed to create PrivateKey object", e);
                    throw new SQLException("Failed to create PrivateKey object: " + e.getMessage(), e);
                }

                // Use DriverManager directly for private key authentication
                return DriverManager.getConnection(processedConnectionString, connectionProps);
            }
            // For password authentication, we can use the parent class
            else {
                LOGGER.info("Using password authentication");

                String connectionString = System.getenv("default");
                String secretName = getSecretName(connectionString);

                DatabaseConnectionConfig tempConfig = new DatabaseConnectionConfig(
                        databaseConnectionConfig.getCatalog(),
                        databaseConnectionConfig.getEngine(),
                        databaseConnectionConfig.getJdbcConnectionString(),
                        secretName);

                GenericJdbcConnectionFactory tempFactory = new GenericJdbcConnectionFactory(
                        tempConfig, properties, databaseConnectionInfo);

                // Use the parent class's getConnection method for password authentication
                return tempFactory.getConnection(credentialsProvider);
            }
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                LOGGER.error("SQL Exception during connection: {}", e.getMessage(), e);
                LOGGER.error("SQL State: {}, Error Code: {}", ((SQLException) e).getSQLState(), ((SQLException) e).getErrorCode());
                throw (SQLException) e;
            }
            LOGGER.error("Exception during connection: {}", e.getMessage(), e);
            throw new SQLException("Failed to establish connection: " + e.getMessage(), e);
        }
    }

    /**
     * Processes the connection string to handle Snowflake-specific formats and remove placeholders.
     *
     * @param connectionString Original connection string
     * @return Processed connection string
     */
    private String processConnectionString(String connectionString)
    {
        // Remove ${...} or ${{...}} placeholders
        if (connectionString.contains("${")) {
            connectionString = connectionString.replaceAll("\\$\\{[^}]*\\}", "");
            LOGGER.debug("Removed placeholders from connection string");
        }

        // Remove authentication related parameters
        connectionString = connectionString.replaceAll("&?secret=[^&]*", "");

        // Fix consecutive &
        connectionString = connectionString.replaceAll("&&", "&");
        // Remove trailing ? or &
        connectionString = connectionString.replaceAll("[?&]$", "");
        // If there's no ? but there are parameters starting with &, convert first & to ?
        if (!connectionString.contains("?") && connectionString.contains("&")) {
            connectionString = connectionString.replaceFirst("&", "?");
        }

        // Handle snowflake://jdbc:snowflake:// format
        if (connectionString.startsWith("snowflake://jdbc:snowflake://")) {
            // Remove snowflake:// prefix to keep only jdbc:snowflake://
            connectionString = connectionString.substring("snowflake://".length());
            LOGGER.debug("Removed 'snowflake://' prefix from connection string");
        }

        return connectionString;
    }
}
