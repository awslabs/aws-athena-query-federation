/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

            Class.forName(databaseConnectionInfo.getDriverClassName());
            Properties connectionProps = new Properties();

            // Add all properties
            if (properties != null) {
                connectionProps.putAll(properties);
            }

            connectionProps.put("user", credentialsProvider.getCredential().getUser());

            if (isPrivateKeyAuth) {
                LOGGER.info("Using private key authentication");
                SnowflakePrivateKeyCredentialProvider privateKeyProvider =
                        (SnowflakePrivateKeyCredentialProvider) credentialsProvider;
                try {
                    // Set PrivateKey object instead of string
                    PrivateKey privateKeyObj = privateKeyProvider.getPrivateKeyObject();
                    connectionProps.put("privateKey", privateKeyObj);
                    LOGGER.debug("Private key object created successfully");

                    // Remove password property (to avoid confusion)
                    connectionProps.remove("password");
                }
                catch (Exception e) {
                    LOGGER.error("Failed to create PrivateKey object", e);
                    throw new SQLException("Failed to create PrivateKey object: " + e.getMessage(), e);
                }
            }
            else {
                // Normal authentication
                connectionProps.put("password", credentialsProvider.getCredential().getPassword());
            }

            // Get original connection string
            String originalConnectionString = databaseConnectionConfig.getJdbcConnectionString();
            LOGGER.debug("Original connection string: {}", originalConnectionString);

            // Modify connection string
            String connectionString = originalConnectionString;

            // Remove ${...} or ${{...}} placeholders
            if (connectionString.contains("${")) {
                // Use safe regex replacement
                connectionString = connectionString.replaceAll("\\$\\{[^}]*\\}", "");
                connectionString = connectionString.replaceAll("\\$\\{\\{[^}]*\\}\\}", "");
                LOGGER.debug("Removed placeholders from connection string");
            }

            // Remove authentication related parameters
            connectionString = connectionString.replaceAll("&?secret_name=[^&]*", "");
            connectionString = connectionString.replaceAll("&?auth_type=[^&]*", "");

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
            // Handle snowflake:// format
            else if (connectionString.startsWith("snowflake://")) {
                // Replace snowflake:// with jdbc:snowflake://
                connectionString = "jdbc:snowflake://" + connectionString.substring("snowflake://".length());
                LOGGER.debug("Replaced 'snowflake://' with 'jdbc:snowflake://' in connection string");
            }
            // If not starting with jdbc:
            else if (!connectionString.startsWith("jdbc:")) {
                // Add jdbc:snowflake:// prefix
                connectionString = "jdbc:snowflake://" + connectionString;
                LOGGER.debug("Added 'jdbc:snowflake://' prefix to connection string");
            }

            return DriverManager.getConnection(connectionString, connectionProps);
        }
        catch (ClassNotFoundException e) {
            LOGGER.error("Driver class not found", e);
            throw new RuntimeException(e);
        }
        catch (SQLException e) {
            LOGGER.error("SQL Exception during connection: {}", e.getMessage(), e);
            LOGGER.error("SQL State: {}, Error Code: {}", e.getSQLState(), e.getErrorCode());
            throw e;
        }
    }
}
