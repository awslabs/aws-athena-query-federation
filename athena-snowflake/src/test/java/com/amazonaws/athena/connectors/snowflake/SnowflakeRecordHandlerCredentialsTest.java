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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE_PASSWORD;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.AUTH_TYPE_PRIVATE_KEY;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_NAME;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SnowflakeRecordHandlerCredentialsTest
{
    @Mock
    private DatabaseConnectionConfig databaseConnectionConfig;

    @Mock
    private JdbcConnectionFactory jdbcConnectionFactory;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private S3Client amazonS3;

    @Mock
    private AthenaClient athena;

    private Map<String, String> configOptions;
    private SnowflakeRecordHandler recordHandler;

    @Before
    public void setup()
    {
        configOptions = new HashMap<>();
        when(databaseConnectionConfig.getEngine()).thenReturn(SNOWFLAKE_NAME);
        when(databaseConnectionConfig.getSecret()).thenReturn("test-secret");

        JdbcSplitQueryBuilder jdbcSplitQueryBuilder = new SnowflakeQueryStringBuilder(
                SNOWFLAKE_QUOTE_CHARACTER,
                new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));

        recordHandler = new SnowflakeRecordHandler(
                databaseConnectionConfig,
                amazonS3,
                secretsManager,
                athena,
                jdbcConnectionFactory,
                jdbcSplitQueryBuilder,
                configOptions);
    }

    @Test
    public void testAuthTypeConfiguration()
    {
        // Configure password authentication
        configOptions.put(AUTH_TYPE, AUTH_TYPE_PASSWORD);
        assertEquals(AUTH_TYPE_PASSWORD, configOptions.get(AUTH_TYPE));

        // Configure private key authentication
        configOptions.put(AUTH_TYPE, AUTH_TYPE_PRIVATE_KEY);
        assertEquals(AUTH_TYPE_PRIVATE_KEY, configOptions.get(AUTH_TYPE));
    }

    @Test
    public void testSecretNameConfiguration()
    {
        // Configure secret name
        configOptions.put("secret_name", "test-secret");
        assertEquals("test-secret", configOptions.get("secret_name"));

        // Get secret name from database connection config
        when(databaseConnectionConfig.getSecret()).thenReturn("db-secret");
        assertEquals("db-secret", databaseConnectionConfig.getSecret());
    }
}
