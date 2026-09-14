/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides a generic jdbc connection factory that can be used to connect to standard databases.
 * When running as a Glue managed connection (configOptions contains {@code fas_token}),
 * uses direct DriverManager connections instead of HikariCP connection pooling.
 */
public class GenericJdbcConnectionFactory
        implements JdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcConnectionFactory.class);
    private static final String SECRET_NAME_PATTERN_STRING = "(\\$\\{[a-zA-Z0-9:/_+=.@!-]+})";
    protected static final Pattern SECRET_NAME_PATTERN = Pattern.compile(SECRET_NAME_PATTERN_STRING);

    protected final DatabaseConnectionConfig databaseConnectionConfig;
    protected final Properties jdbcProperties;
    protected final DatabaseConnectionInfo databaseConnectionInfo;
    
    private final boolean useDirectConnection;
    private volatile HikariDataSource ds;
    /** Hash of the JDBC URL and credentials the current pool was built for. */
    private volatile String poolIdentityKey;

    /**
     * Existing constructor — defaults to no configOptions (no Glue managed connection).
     */
    public GenericJdbcConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig, final Map<String, String> properties, final DatabaseConnectionInfo databaseConnectionInfo)
    {
        this(databaseConnectionConfig, properties, databaseConnectionInfo, null);
    }

    /**
     * @param databaseConnectionConfig database connection configuration {@link DatabaseConnectionConfig}
     * @param properties JDBC connection properties.
     * @param databaseConnectionInfo Contains JDBC driver and default port details.
     * @param configOptions environment/config options; when {@code fas_token} is present, uses direct DriverManager connections.
     */
    public GenericJdbcConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig, final Map<String, String> properties, final DatabaseConnectionInfo databaseConnectionInfo, final Map<String, String> configOptions)
    {
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseEngine must not be null");

        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }

        this.useDirectConnection = configOptions != null && configOptions.containsKey(EnvironmentConstants.FAS_TOKEN);
        if (this.useDirectConnection) {
            LOGGER.info("Glue managed connection detected, using direct JDBC connections");
            try {
                Class.forName(databaseConnectionInfo.getDriverClassName());
            }
            catch (ClassNotFoundException e) {
                throw new AthenaConnectorException("JDBC driver not found: " + databaseConnectionInfo.getDriverClassName(),
                        ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
            }
        }
    }

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider)
            throws Exception
    {
        if (useDirectConnection) {
            return createDirectConnection(credentialsProvider);
        }

        return getConnectionFromManagedPool(credentialsProvider);
    }

    /**
     * Returns a connection from the HikariCP connection pool, initializing the pool on first call.
     * Subclasses can call this directly to bypass the {@code useDirectConnection} check.
     *
     * @param credentialsProvider jdbc user and password provider, or null if no credentials.
     * @return JDBC connection from the pool.
     * @throws SQLException if a connection cannot be obtained.
     */
    protected Connection getConnectionFromManagedPool(final CredentialsProvider credentialsProvider) throws SQLException
    {
        final String derivedJdbcString = getDerivedJdbcString(credentialsProvider);
        // HikariCP snapshots the credentials at pool-construction time (setDataSourceProperties
        // copies them) and rejects per-call credentials with SQLFeatureNotSupportedException. A
        // pool built for one identity would therefore keep serving that identity's connections
        // even after a later request merged different credentials into jdbcProperties. Rebuild
        // the pool when the effective identity changes so a request never borrows another's
        // credentials. This matters for multi-tenant callers, where connectors such as Snowflake
        // stay pooled even when fas_token is present.
        final String poolIdentity = buildPoolIdentity(derivedJdbcString);

        HikariDataSource dataSource = ds;
        if (dataSource == null || dataSource.isClosed() || !poolIdentity.equals(poolIdentityKey)) {
            // Lock on this instance, not on the class: each factory owns its own pool, so a
            // class-level lock made unrelated factories (e.g. different multiplexed catalogs)
            // serialize behind each other while a pool was being built.
            synchronized (this) {
                if (ds == null || ds.isClosed() || !poolIdentity.equals(poolIdentityKey)) { // Double-check to avoid creating more than one instance
                    if (ds != null && !ds.isClosed()) {
                        LOGGER.debug("Connection identity changed; replacing existing data source");
                        ds.close();
                    }
                    HikariConfig config2 = new HikariConfig();
                    config2.setDriverClassName(databaseConnectionInfo.getDriverClassName());
                    config2.setDataSourceProperties(jdbcProperties);
                    config2.setJdbcUrl(derivedJdbcString);
                    config2.setMinimumIdle(1);
                    ds = new HikariDataSource(config2);
                    poolIdentityKey = poolIdentity;
                    LOGGER.debug("Create data source");
                }
                dataSource = ds;
            }
        }

        try {
            return dataSource.getConnection();
        }
        catch (SQLException e) {
            handleSQLExceptionWhenGetConnection(e);
            throw e;
        }
    }

    /**
     * Builds an opaque key identifying the connection identity a pool was created for: the JDBC
     * URL plus the credential-bearing properties. Values are hashed so credentials are never
     * retained in memory in plaintext or exposed through logs.
     */
    private String buildPoolIdentity(final String derivedJdbcString)
    {
        StringBuilder identity = new StringBuilder(derivedJdbcString);
        for (String key : new TreeSet<>(jdbcProperties.stringPropertyNames())) {
            identity.append('\n').append(key).append('=').append(jdbcProperties.getProperty(key));
        }

        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(identity.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        }
        catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM, so this is unreachable in practice.
            throw new AthenaConnectorException("SHA-256 is unavailable",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
    }

    /**
     * Creates a direct JDBC connection using {@link DriverManager}, bypassing connection pooling.
     * Used for Glue managed connections.
     */
    private Connection createDirectConnection(final CredentialsProvider credentialsProvider) throws SQLException
    {
        final String derivedJdbcString = getDerivedJdbcString(credentialsProvider);

        Properties connectionProps = new Properties();
        connectionProps.putAll(jdbcProperties);
        try {
            return DriverManager.getConnection(derivedJdbcString, connectionProps);
        }
        catch (SQLException e) {
            handleSQLExceptionWhenGetConnection(e);
            throw e;
        }
    }

    /**
     * Derives the JDBC connection string by stripping secret placeholders and merging
     * credentials into {@link #jdbcProperties}.
     * <p>
     * Note: URL encoding in the connection string is passed through as-is. The underlying
     * JDBC driver is responsible for decoding (e.g. Snowflake's driver decodes %22 to ").
     *
     * @param credentialsProvider credential provider, or null if no credentials.
     * @return the derived JDBC connection string.
     */
    private String getDerivedJdbcString(final CredentialsProvider credentialsProvider)
    {
        final String derivedJdbcString;
        if (credentialsProvider != null) {
            Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
            derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(""));

            jdbcProperties.putAll(credentialsProvider.getCredentialMap());
        }
        else {
            derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
        }
        return derivedJdbcString;
    }

    private void handleSQLExceptionWhenGetConnection(final SQLException e) throws SQLException
    {
        if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("Name or service not known")) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        else if (StringUtils.isNotBlank(e.getMessage()) && e.getMessage().contains("Incorrect username or password was specified.")) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString()).build());
        }
        throw e;
    }

    /**
     * Closes the connection pool, releasing pooled connections and their background threads.
     * The factory stays usable: a later {@code getConnection} rebuilds the pool, so closing a
     * handler that is subsequently reused does not permanently break it.
     */
    @Override
    public void close()
    {
        synchronized (this) {
            if (ds != null) {
                ds.close();
                ds = null;
                poolIdentityKey = null;
            }
        }
    }
}
