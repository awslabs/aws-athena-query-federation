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
package com.amazonaws.connectors.athena.jdbc.connection;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides a generic jdbc connection factory that can be used to connect to standard databases. Configures following
 * defaults if not present:
 * <ul>
 * <li>Default ports will be used for the engine if not present.</li>
 * </ul>
 */
public class GenericJdbcConnectionFactory
        implements JdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcConnectionFactory.class);

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final int MYSQL_DEFAULT_PORT = 3306;

    private static final String POSTGRESQL_DRIVER_CLASS = "org.postgresql.Driver";
    private static final int POSTGRESQL_DEFAULT_PORT = 5432;

    private static final String REDSHIFT_DRIVER_CLASS = "com.amazon.redshift.jdbc.Driver";
    private static final int REDSHIFT_DEFAULT_PORT = 5439;

    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final int ORACLE_DEFAULT_PORT = 1521;

    private static final String SQL_SERVER_DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final int SQL_SERVER_DEFAULT_PORT = 1433;

    private static final String SECRET_NAME_PATTERN_STRING = "(\\$\\{[a-zA-Z0-9/_+=.@-]+})";
    public static final Pattern SECRET_NAME_PATTERN = Pattern.compile(SECRET_NAME_PATTERN_STRING);

    private static final ImmutableMap<DatabaseEngine, DatabaseConnectionInfo> CONNECTION_INFO = ImmutableMap.of(
            DatabaseEngine.MYSQL, new DatabaseConnectionInfo(MYSQL_DRIVER_CLASS, MYSQL_DEFAULT_PORT),
            DatabaseEngine.POSTGRES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT),
            DatabaseEngine.REDSHIFT, new DatabaseConnectionInfo(REDSHIFT_DRIVER_CLASS, REDSHIFT_DEFAULT_PORT),
            DatabaseEngine.ORACLE, new DatabaseConnectionInfo(ORACLE_DRIVER_CLASS, ORACLE_DEFAULT_PORT),
            DatabaseEngine.SQLSERVER, new DatabaseConnectionInfo(SQL_SERVER_DRIVER_CLASS, SQL_SERVER_DEFAULT_PORT));

    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;

    public GenericJdbcConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig, final Map<String, String> properties)
    {
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseEngine must not be null");

        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
    }

    @Override
    public Connection getConnection(final JdbcCredentialProvider jdbcCredentialProvider)
    {
        try {
            DatabaseConnectionInfo databaseConnectionInfo = CONNECTION_INFO.get(this.databaseConnectionConfig.getType());

            final String derivedJdbcString;
            if (jdbcCredentialProvider != null) {
                Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                final String secretReplacement = String.format("user=%s&password=%s", jdbcCredentialProvider.getCredential().getUser(),
                        jdbcCredentialProvider.getCredential().getPassword());
                derivedJdbcString = secretMatcher.replaceAll(secretReplacement);
            }
            else {
                derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
            }

            // create connection string
            LOGGER.info("Connection string {}", derivedJdbcString);

            // register driver
            Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();

            // create connection
            return DriverManager.getConnection(derivedJdbcString, this.jdbcProperties);
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException);
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            throw new RuntimeException(ex);
        }
    }
}
