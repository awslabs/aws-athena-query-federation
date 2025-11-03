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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

public class OracleJdbcConnectionFactory extends GenericJdbcConnectionFactory
{
    public static final String IS_FIPS_ENABLED = "is_fips_enabled";
    public static final String IS_FIPS_ENABLED_LEGACY = "is_FIPS_Enabled";
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleJdbcConnectionFactory.class);

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
    public Connection getConnection(final CredentialsProvider credentialsProvider)
    {
        try {
            final String derivedJdbcString;
            Properties properties = new Properties();

            if (null != credentialsProvider) {
                //checking for tcps (Secure Communication) protocol as part of the connection string.
                if (databaseConnectionConfig.getJdbcConnectionString().toLowerCase().contains("@tcps://")) {
                    LOGGER.info("Establishing connection over SSL..");
                    properties.put("javax.net.ssl.trustStoreType", "JKS");
                    properties.put("javax.net.ssl.trustStorePassword", "changeit");
                    properties.put("oracle.net.ssl_server_dn_match", "true");
                    // By default; Oracle RDS uses SSL_RSA_WITH_AES_256_CBC_SHA
                    // Adding the following cipher suits to support others listed in Doc
                    // https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.Options.SSL.html#Appendix.Oracle.Options.SSL.CipherSuites
                    if (getEnvMap().getOrDefault(IS_FIPS_ENABLED, "false").equalsIgnoreCase("true") || getEnvMap().getOrDefault(IS_FIPS_ENABLED_LEGACY, "false").equalsIgnoreCase("true")) {
                        properties.put("oracle.net.ssl_cipher_suites", "(TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA)");
                    }
                }
                else {
                    LOGGER.info("Establishing normal connection..");
                }
                Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                String password = credentialsProvider.getCredentialMap().get(CredentialsConstants.PASSWORD);
                if (!password.contains("\"")) {
                    password = String.format("\"%s\"", password);
                }
                final String secretReplacement = String.format("%s/%s", credentialsProvider.getCredentialMap().get(CredentialsConstants.USER),
                        password);
                derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(secretReplacement));
                // register driver
                Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();
                return DriverManager.getConnection(derivedJdbcString, properties);
            }
            else {
                throw new RuntimeException("Invalid connection string, Secret name is required.");
            }
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException);
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Extracted method for environment variables to allow overriding in tests.
     */
    @VisibleForTesting
    protected Map<String, String> getEnvMap()
    {
        return System.getenv();
    }
}
