/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import org.apache.commons.lang3.Validate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

public class SynapseJdbcConnectionFactory extends GenericJdbcConnectionFactory
{
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;

    /**
     * @param databaseConnectionConfig database connection configuration {@link DatabaseConnectionConfig}
     * @param properties               JDBC connection properties.
     * @param databaseConnectionInfo
     */
    public SynapseJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> properties, DatabaseConnectionInfo databaseConnectionInfo)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseEngine must not be null");
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
    }

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider)
    {
        try {
            final String derivedJdbcString;
            final Properties connectionProps = new Properties();
            connectionProps.putAll(this.jdbcProperties);

            if (credentialsProvider != null) {
                Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
                final String secretReplacement;

                String connectionString = databaseConnectionConfig.getJdbcConnectionString();

                if (connectionString.contains("authentication=ActiveDirectoryServicePrincipal")) {
                    // AAD Service Principal credentials
                    DefaultCredentials credentials = credentialsProvider.getCredential();
                    secretReplacement = String.format(
                            "%s;%s",
                            "AADSecurePrincipalId=" + credentials.getUser(),
                            "AADSecurePrincipalSecret=" + credentials.getPassword()
                    );
                }
                else {
                    SynapseCredentialsProvider synapseProvider = (SynapseCredentialsProvider) credentialsProvider;
                    String accessToken = synapseProvider.getOAuthAccessToken();

                    if (accessToken != null) {
                        // OAuth token
                        connectionProps.setProperty("accessToken", accessToken);
                        secretReplacement = "";
                    }
                    else {
                        // Fallback to username/password and change username as user
                        DefaultCredentials credentials = synapseProvider.getCredential();
                        secretReplacement = String.format(
                                "%s;%s",
                                "user=" + credentials.getUser(),
                                "password=" + credentials.getPassword()
                        );
                    }
                }

                derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(secretReplacement));
            }
            else {
                derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
            }
            // register driver
            Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();
            // create connection
            return DriverManager.getConnection(derivedJdbcString, connectionProps);
        }
        catch (SQLException sqlException) {
            throw new RuntimeException(sqlException.getErrorCode() + ": " + sqlException);
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            throw new RuntimeException(ex);
        }
    }
}
