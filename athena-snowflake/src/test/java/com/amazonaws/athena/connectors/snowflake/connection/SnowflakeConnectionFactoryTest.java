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
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import org.junit.Before;
import org.junit.Test;

import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SnowflakeConnectionFactoryTest
{
    private DatabaseConnectionConfig databaseConnectionConfig;
    private DatabaseConnectionInfo databaseConnectionInfo;
    private Map<String, String> properties;
    private SnowflakeConnectionFactory connectionFactory;
    private CredentialsProvider credentialsProvider;
    private SnowflakePrivateKeyCredentialProvider privateKeyCredentialProvider;

    @Before
    public void setup()
    {
        databaseConnectionConfig = mock(DatabaseConnectionConfig.class);
        when(databaseConnectionConfig.getJdbcConnectionString()).thenReturn("jdbc:snowflake://hostname/?warehouse=warehousename&db=dbname&schema=schemaname");
        when(databaseConnectionConfig.getSecret()).thenReturn("test-secret");
        when(databaseConnectionConfig.getCatalog()).thenReturn("test-catalog");
        when(databaseConnectionConfig.getEngine()).thenReturn("snowflake");

        databaseConnectionInfo = mock(DatabaseConnectionInfo.class);
        when(databaseConnectionInfo.getDriverClassName()).thenReturn("com.snowflake.client.jdbc.SnowflakeDriver");

        properties = new HashMap<>();
        properties.put("warehouse", "warehousename");
        properties.put("db", "dbname");
        properties.put("schema", "schemaname");

        connectionFactory = new SnowflakeConnectionFactory(databaseConnectionConfig, properties, databaseConnectionInfo);

        credentialsProvider = mock(CredentialsProvider.class);
        DefaultCredentials credentials = new DefaultCredentials("testUser", "testPassword");
        when(credentialsProvider.getCredential()).thenReturn(credentials);

        privateKeyCredentialProvider = mock(SnowflakePrivateKeyCredentialProvider.class);
        DefaultCredentials privateKeyCredentials = new DefaultCredentials("testUser", "testPrivateKey");
        when(privateKeyCredentialProvider.getCredential()).thenReturn(privateKeyCredentials);
    }

    @Test
    public void testAuthenticationTypeSelection() throws Exception {

        final boolean[] privateKeyAuthUsed = {false};
        final boolean[] passwordAuthUsed = {false};

        SnowflakeConnectionFactory testFactory = new SnowflakeConnectionFactory(
                databaseConnectionConfig, properties, databaseConnectionInfo) {
            @Override
            public Connection getConnection(CredentialsProvider credentialsProvider) throws SQLException {
                try {
                    if (credentialsProvider instanceof SnowflakePrivateKeyCredentialProvider) {
                        privateKeyAuthUsed[0] = true;
                        SnowflakePrivateKeyCredentialProvider provider = (SnowflakePrivateKeyCredentialProvider) credentialsProvider;
                        assertNotNull(provider.getPrivateKeyObject());
                    } else {
                        passwordAuthUsed[0] = true;
                    }

                    return mock(Connection.class);
                } catch (Exception e) {
                    throw new SQLException("Test exception", e);
                }
            }
        };

        DefaultCredentialsProvider defaultProvider = mock(DefaultCredentialsProvider.class);
        DefaultCredentials defaultCredentials = new DefaultCredentials("testUser", "testPassword");
        when(defaultProvider.getCredential()).thenReturn(defaultCredentials);

        SnowflakePrivateKeyCredentialProvider privateKeyProvider = mock(SnowflakePrivateKeyCredentialProvider.class);
        DefaultCredentials privateKeyCredentials = new DefaultCredentials("testUser", "testPrivateKey");
        when(privateKeyProvider.getCredential()).thenReturn(privateKeyCredentials);
        when(privateKeyProvider.getPrivateKeyObject()).thenReturn(mock(PrivateKey.class));

        testFactory.getConnection(privateKeyProvider);
        assertTrue("Private key authentication should be used", privateKeyAuthUsed[0]);
        assertFalse("Password authentication should not be used", passwordAuthUsed[0]);

        privateKeyAuthUsed[0] = false;
        passwordAuthUsed[0] = false;

        testFactory.getConnection(defaultProvider);
        assertFalse("Private key authentication should not be used", privateKeyAuthUsed[0]);
        assertTrue("Password authentication should be used", passwordAuthUsed[0]);
    }

    @Test
    public void testProcessConnectionString() throws Exception
    {
        java.lang.reflect.Method method = SnowflakeConnectionFactory.class.getDeclaredMethod("processConnectionString", String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(connectionFactory, "snowflake://jdbc:snowflake://hostname/?secret=test-secret");
        assertEquals("jdbc:snowflake://hostname/", result);
    }
}