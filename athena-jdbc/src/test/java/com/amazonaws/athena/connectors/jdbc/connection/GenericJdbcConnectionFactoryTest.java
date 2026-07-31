/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connectors.jdbc.credentials.RdsIamAuthConfiguration;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import java.sql.Connection;
import java.util.regex.Matcher;

import static com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory.SECRET_NAME_PATTERN;

public class GenericJdbcConnectionFactoryTest
{
    private static final String CATALOG = "default";
    private static final String ENGINE = "postgres";
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 5432;
    private static final String USERNAME = "test_user";
    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static final RdsIamAuthConfiguration IAM_AUTH_CONFIGURATION = new RdsIamAuthConfiguration(
            HOSTNAME,
            PORT,
            USERNAME,
            Region.US_EAST_1);

    @Test
    public void matchSecretNamePattern_WhenValidSecretPresent_MatchesSecret()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertTrue(secretMatcher.find());
    }

    @Test
    public void matchIncorrectSecretNamePattern_WhenInvalidCharactersPresent_DoesNotMatch()
    {
        String jdbcConnectionString = "mysql://jdbc:mysql://mysql.host:3333/default?${secret!@+=*_}";
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(jdbcConnectionString);

        Assert.assertFalse(secretMatcher.find());
    }

    @Test
    public void constructor_WhenIamAuthEnabled_LoadsJdbcDriver()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:h2:mem:iamAuthFactory;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                IAM_AUTH_CONFIGURATION);

        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(
                databaseConnectionConfig,
                null,
                new DatabaseConnectionInfo(DRIVER_CLASS, PORT),
                null);

        Assert.assertNotNull(factory);
    }

    @Test
    public void getConnection_WhenIamAuthEnabled_OpensDirectConnection() throws Exception
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:h2:mem:iamAuthDirect;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
                IAM_AUTH_CONFIGURATION);
        GenericJdbcConnectionFactory factory = new GenericJdbcConnectionFactory(
                databaseConnectionConfig,
                null,
                new DatabaseConnectionInfo(DRIVER_CLASS, PORT),
                null);
        CredentialsProvider credentialsProvider = () -> new DefaultCredentials(USERNAME, "iam-token");

        try (Connection connection = factory.getConnection(credentialsProvider)) {
            Assert.assertNotNull(connection);
            Assert.assertFalse(connection.isClosed());
        }
    }
}
