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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OracleJdbcConnectionFactoryTest
{
    private static final String CONNECTION_STRING = "oracle://jdbc:oracle:thin:username/password@//test.oracle.com:1521/orcl";
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SECRET = "test";
    private static final String CATALOG = "catalog";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";


    @Test(expected = RuntimeException.class)
    public void testGetConnection() throws SQLException
    {
        DefaultCredentials expectedCredential = new DefaultCredentials(USERNAME, PASSWORD);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(expectedCredential);
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(TEST_CATALOG, OracleConstants.ORACLE_NAME,
                CONNECTION_STRING, TEST_SECRET);
        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS, OracleConstants.ORACLE_DEFAULT_PORT);
        Connection connection =  new OracleJdbcConnectionFactory(databaseConnectionConfig, databaseConnectionInfo).getConnection(credentialsProvider);
        String originalURL = connection.getMetaData().getURL();
        Driver drv = DriverManager.getDriver(originalURL);
        String driverClass = drv.getClass().getName();
        assertEquals("oracle.jdbc.OracleDriver", driverClass);
    }

    @Test
    public void testGetConnection_withSsl() throws Exception {
        Driver mockDriver = mock(Driver.class);

        DefaultCredentials expectedCredential = new DefaultCredentials(USERNAME, PASSWORD);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(expectedCredential);

        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig(
                TEST_CATALOG,
                OracleConstants.ORACLE_NAME,
                "oracle://jdbc:oracle:thin:username/password@tcps://test.oracle.com:1521/orcl",
                TEST_SECRET
        );

        DatabaseConnectionInfo databaseConnectionInfo = new DatabaseConnectionInfo(
                OracleConstants.ORACLE_DRIVER_CLASS,
                OracleConstants.ORACLE_DEFAULT_PORT
        );

        Properties[] capturedProps = new Properties[1];

        when(mockDriver.acceptsURL(anyString())).thenReturn(true);
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenAnswer(invocation -> {
                    capturedProps[0] = invocation.getArgument(1);
                    return mock(Connection.class);
                });

        DriverManager.registerDriver(mockDriver);

        // Create a test connection factory with custom environment
        OracleJdbcConnectionFactory connectionFactory = new OracleJdbcConnectionFactory(databaseConnectionConfig, databaseConnectionInfo) {
            @Override
            protected Map<String, String> getEnvMap() {
                Map<String, String> env = new HashMap<>();
                env.put(IS_FIPS_ENABLED, "true");
                return env;
            }
        };

        connectionFactory.getConnection(credentialsProvider);

        Properties sslProps = capturedProps[0];
        assertEquals("JKS", sslProps.getProperty("javax.net.ssl.trustStoreType"));
        assertEquals("changeit", sslProps.getProperty("javax.net.ssl.trustStorePassword"));
        assertEquals("true", sslProps.getProperty("oracle.net.ssl_server_dn_match"));
        assertEquals(
                "(TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA)",
                sslProps.getProperty("oracle.net.ssl_cipher_suites")
        );

        DriverManager.deregisterDriver(mockDriver);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_withNullCredentialsProvider_throwsRuntimeException()
    {
        DatabaseConnectionConfig config = new DatabaseConnectionConfig(
                TEST_CATALOG, OracleConstants.ORACLE_NAME,
                CONNECTION_STRING, TEST_SECRET);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS, OracleConstants.ORACLE_DEFAULT_PORT);

        new OracleJdbcConnectionFactory(config, info).getConnection(null);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_whenDriverFailsToConnect_throwsRuntimeException() throws Exception
    {
        DefaultCredentials credentials = new DefaultCredentials(USERNAME, PASSWORD);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

        DatabaseConnectionConfig config = new DatabaseConnectionConfig(CATALOG, OracleConstants.ORACLE_NAME, CONNECTION_STRING, TEST_SECRET);
        DatabaseConnectionInfo info = new DatabaseConnectionInfo(OracleConstants.ORACLE_DRIVER_CLASS, OracleConstants.ORACLE_DEFAULT_PORT);

        Driver mockDriver = mock(Driver.class);
        when(mockDriver.acceptsURL(anyString())).thenReturn(true);
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenThrow(new SQLException("DB down", "08001", 1234));

        DriverManager.registerDriver(mockDriver);

            new OracleJdbcConnectionFactory(config, info).getConnection(credentialsProvider);
            DriverManager.deregisterDriver(mockDriver);
    }

    @Test(expected = RuntimeException.class)
    public void getConnection_whenDriverClassNotFound_throwsRuntimeException()
    {
        DefaultCredentials credentials = new DefaultCredentials(USERNAME, PASSWORD);
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

        DatabaseConnectionConfig config = new DatabaseConnectionConfig(CATALOG, OracleConstants.ORACLE_NAME, CONNECTION_STRING, TEST_SECRET);

        // Provide an invalid/non-existent driver class name to trigger ClassNotFoundException
        DatabaseConnectionInfo info = new DatabaseConnectionInfo("non.existent.DriverClass", OracleConstants.ORACLE_DEFAULT_PORT);

        new OracleJdbcConnectionFactory(config, info).getConnection(credentialsProvider);
    }
}
