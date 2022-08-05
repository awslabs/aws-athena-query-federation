/*-
 * #%L
 * athena-oracle
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

package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcCredentialProvider;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleJdbcConnectionFactory extends GenericJdbcConnectionFactory
{
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleJdbcConnectionFactory.class);
    private static final String SSL_CONNECTION_STRING_REGEX = "jdbc:oracle:thin:\\$\\{([a-zA-Z0-9:_/+=.@-]+)\\}@" +
            "\\((?i)description=\\(address=\\(protocol=tcps\\)\\(host=[a-zA-Z0-9-.]+\\)" +
            "\\(port=([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])\\)\\)" +
            "\\(connect_data=\\(sid=[a-zA-Z_]+\\)\\)\\(security=\\(ssl_server_cert_dn=\"[=a-zA-Z,0-9-.,]+\"\\)\\)\\)";
    private static final Pattern SSL_CONNECTION_STRING_PATTERN = Pattern.compile(SSL_CONNECTION_STRING_REGEX);

    /**
     * @param databaseConnectionConfig database connection configuration {@link DatabaseConnectionConfig}
     * @param databaseConnectionInfo
     */
    public OracleJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, DatabaseConnectionInfo databaseConnectionInfo)
    {
        super(databaseConnectionConfig, null, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseEngine must not be null");
    }

    @Override
    public Connection getConnection(final JdbcCredentialProvider jdbcCredentialProvider)
    {
        try {
            final String derivedJdbcString;
            Properties properties = new Properties();

            if (null != jdbcCredentialProvider) {
                if (SSL_CONNECTION_STRING_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString()).matches()) {
                    LOGGER.info("Establishing connection over SSL..");
                    properties.put("javax.net.ssl.trustStoreType", "JKS");
                    properties.put("javax.net.ssl.trustStorePassword", "changeit");
                    properties.put("oracle.net.ssl_server_dn_match", "true");
                }
                else {
                    LOGGER.info("Establishing normal connection..");
                }
                Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                final String secretReplacement = String.format("%s/%s", jdbcCredentialProvider.getCredential().getUser(),
                        jdbcCredentialProvider.getCredential().getPassword());
                derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(secretReplacement));
                LOGGER.info("derivedJdbcString: " + derivedJdbcString);
                return DriverManager.getConnection(derivedJdbcString, properties);
            }
            else {
                throw new RuntimeException("Invalid connection string, Secret name is required.");
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException);
        }
    }
}
