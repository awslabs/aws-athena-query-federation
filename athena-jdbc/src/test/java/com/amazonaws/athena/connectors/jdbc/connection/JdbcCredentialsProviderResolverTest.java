/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
import com.amazonaws.athena.connector.credentials.RdsIamAuthConfiguration;
import com.amazonaws.athena.connector.credentials.RdsIamCredentialsProvider;
import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JdbcCredentialsProviderResolverTest
{
    private static final String CATALOG = "default";
    private static final String ENGINE = "postgres";
    private static final String HOSTNAME = "mydb.us-east-1.rds.amazonaws.com";
    private static final int PORT = 5432;
    private static final String USERNAME = "athena_iam";

    private static final RdsIamAuthConfiguration IAM_AUTH_CONFIGURATION = new RdsIamAuthConfiguration(
            HOSTNAME,
            PORT,
            USERNAME,
            Region.US_EAST_1);

    @Mock
    private FederationRequestHandler handler;

    @Mock
    private CredentialsProvider secretCredentialsProvider;

    @Mock
    private AwsRequestOverrideConfiguration requestOverrideConfiguration;

    @After
    public void tearDown()
    {
        JdbcCredentialsProviderResolver.clearIamProviderCache();
    }

    @Test
    public void resolve_WhenIamAuthEnabled_ReturnsRdsIamCredentialsProvider()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);

        CredentialsProvider credentialsProvider = JdbcCredentialsProviderResolver.resolve(
                databaseConnectionConfig,
                handler,
                requestOverrideConfiguration);

        assertTrue(credentialsProvider instanceof RdsIamCredentialsProvider);
    }

    @Test
    public void resolve_WhenIamAuthEnabledTwice_ReusesSameRdsIamCredentialsProvider()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://mydb.us-east-1.rds.amazonaws.com:5432/dev",
                IAM_AUTH_CONFIGURATION);

        CredentialsProvider first = JdbcCredentialsProviderResolver.resolve(
                databaseConnectionConfig, handler, requestOverrideConfiguration);
        CredentialsProvider second = JdbcCredentialsProviderResolver.resolve(
                databaseConnectionConfig, handler, requestOverrideConfiguration);

        assertSame(first, second);
        assertTrue(first instanceof RdsIamCredentialsProvider);
    }

    @Test
    public void resolve_WhenSecretConfigured_ReturnsHandlerCredentialsProvider()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://hostname:5432/dev?${testSecret}",
                "testSecret");
        when(handler.createCredentialsProvider(eq("testSecret"), eq(requestOverrideConfiguration)))
                .thenReturn(secretCredentialsProvider);

        CredentialsProvider credentialsProvider = JdbcCredentialsProviderResolver.resolve(
                databaseConnectionConfig,
                handler,
                requestOverrideConfiguration);

        assertEquals(secretCredentialsProvider, credentialsProvider);
        verify(handler).createCredentialsProvider("testSecret", requestOverrideConfiguration);
    }

    @Test
    public void resolve_WhenNoSecretAndNoIamAuth_ReturnsNull()
    {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                CATALOG,
                ENGINE,
                "jdbc:postgresql://hostname:5432/dev?user=test&password=test");

        CredentialsProvider credentialsProvider = JdbcCredentialsProviderResolver.resolve(
                databaseConnectionConfig,
                handler,
                requestOverrideConfiguration);

        assertNull(credentialsProvider);
    }

    @Test
    public void resolve_WhenConfigNullAndHandlerHasSecret_ReturnsHandlerCredentialsProvider()
    {
        when(handler.getDatabaseConnectionSecret()).thenReturn("handlerSecret");
        when(handler.createCredentialsProvider(eq("handlerSecret"), any(AwsRequestOverrideConfiguration.class)))
                .thenReturn(secretCredentialsProvider);

        CredentialsProvider credentialsProvider = JdbcCredentialsProviderResolver.resolve(
                null,
                handler,
                requestOverrideConfiguration);

        assertEquals(secretCredentialsProvider, credentialsProvider);
    }
}
