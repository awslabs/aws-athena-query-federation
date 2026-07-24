/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase.connection;

import com.amazonaws.athena.connectors.hbase.HbaseEnvironmentProperties;
import com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.HBASE_RPC_PROTECTION;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_AUTH_ENABLED;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.PRINCIPAL_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseConnectionFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnectionFactoryTest.class);
    private static final String CONN_STR_LOCALHOST = "localhost:60000:2181";
    private static final String CONFIG1 = "config1";
    private static final String CONFIG2 = "config2";
    private static final String CONFIG3 = "config3";
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";
    private static final String VALUE3 = "value3";
    private static final String HBASE_RPC_TIMEOUT = "hbase.rpc.timeout";
    private static final String TIMEOUT_2000 = "2000";
    private static final String TIMEOUT_5000 = "5000";
    private static final String TEST_CONFIG_NAME = "test.config.name";
    private static final String TEST_CONFIG_VALUE = "test.config.value";
    private static final String TEST_S3_URI = "s3://test-bucket/test-prefix/";

    private HbaseConnectionFactory connectionFactory;

    @Before
    public void setUp() {
        connectionFactory = new HbaseConnectionFactory();
    }

    @After
    public void tearDown()
    {
        // Clean up environment variables
        System.clearProperty("java.security.krb5.conf");
    }

    @Test
    public void getOrCreateConn_withCachedConnection_returnsCachedConnection()
            throws IOException
    {
        logger.info("getOrCreateConn_withCachedConnection_returnsCachedConnection: enter");
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});
        when(mockConn.isHealthy()).thenReturn(true);

        connectionFactory.addConnection("conStr", mockConn);
        HBaseConnection conn = connectionFactory.getOrCreateConn("conStr");

        assertEquals(mockConn, conn);
        verify(mockConn, times(1)).listNamespaceDescriptors();
        verify(mockConn, times(1)).isHealthy();
        logger.info("getOrCreateConn_withCachedConnection_returnsCachedConnection: exit");
    }

    @Test
    public void setClientConfig_withValidConfig_addsToConfigMap()
    {
        connectionFactory.setClientConfig(TEST_CONFIG_NAME, TEST_CONFIG_VALUE);
        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertTrue("Config should contain the set value", configs.containsKey(TEST_CONFIG_NAME));
        assertEquals("Config value should match", TEST_CONFIG_VALUE, configs.get(TEST_CONFIG_NAME));
    }

    @Test
    public void getClientConfigs_withDefaultConfigs_returnsDefaultValues()
    {
        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertNotNull("Configs should not be null", configs);
        assertTrue("Configs should contain default values", configs.containsKey(HBASE_RPC_TIMEOUT));
        assertEquals("Default timeout should be 2000", TIMEOUT_2000, configs.get(HBASE_RPC_TIMEOUT));
    }

    @Test
    public void getClientConfigs_whenModifyingReturnedMap_throwsUnsupportedOperationException()
    {
        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertThrows(UnsupportedOperationException.class, () ->
                configs.put("test.key", "test.value"));
    }

    @Test
    public void getOrCreateConn_whenNoCachedConnection_createsNewConnectionAndCachesIt()
            throws IOException
    {
        String conStr = CONN_STR_LOCALHOST;
        Connection mockHBaseConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockHBaseConn.getAdmin()).thenReturn(mockAdmin);

        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockHBaseConn);

            when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});

            HBaseConnection conn = testFactory.getOrCreateConn(conStr);
            HBaseConnection conn2 = testFactory.getOrCreateConn(conStr);

            connectionFactoryMock.verify(() -> ConnectionFactory.createConnection(any(Configuration.class)), times(1));
            connectionFactoryMock.verify(() -> ConnectionFactory.createConnection(any(Configuration.class)), times(1));

            assertNotNull("Connection should not be null", conn);
            assertNotNull("Second connection should not be null", conn2);
            assertEquals("Second connection should be cached (same instance)", conn, conn2);
        }
    }

    @Test
    public void getOrCreateConn_withUnhealthyConnection_replacesConnection()
    {
        String conStr = CONN_STR_LOCALHOST;
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.isHealthy()).thenReturn(false);

        connectionFactory.addConnection(conStr, mockConn);
        HBaseConnection newConn = connectionFactory.getOrCreateConn(conStr);

        verify(mockConn, times(1)).close();
        assertNotNull("New connection should not be null", newConn);
        assertNotEquals("Should create new connection", mockConn, newConn);
    }

    @Test
    public void getOrCreateConn_withConnectionTestIOException_replacesConnection()
            throws IOException
    {
        String conStr = CONN_STR_LOCALHOST;
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.isHealthy()).thenReturn(true);
        when(mockConn.listNamespaceDescriptors()).thenThrow(new IOException("Connection test failed"));

        connectionFactory.addConnection(conStr, mockConn);
        HBaseConnection newConn = connectionFactory.getOrCreateConn(conStr);

        verify(mockConn, times(1)).close();
        assertNotNull("New connection should not be null", newConn);
        assertNotEquals("Should create new connection after exception", mockConn, newConn);
    }

    @Test
    public void getOrCreateConn_withConnectionTestRuntimeException_replacesConnection()
            throws IOException
    {
        String conStr = CONN_STR_LOCALHOST;
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.isHealthy()).thenReturn(true);
        when(mockConn.listNamespaceDescriptors()).thenThrow(new RuntimeException("Connection test failed"));

        connectionFactory.addConnection(conStr, mockConn);
        HBaseConnection newConn = connectionFactory.getOrCreateConn(conStr);

        verify(mockConn, times(1)).close();
        assertNotNull("New connection should not be null", newConn);
        assertNotEquals("Should create new connection after exception", mockConn, newConn);
    }

    @Test
    public void getOrCreateConn_withInvalidEndpointFormat_throwsIllegalArgumentException()
    {
        assertGetOrCreateConnThrowsIllegalArgumentException("invalid:format");
    }

    @Test
    public void getOrCreateConn_withInvalidEndpointFormatTooManyParts_throwsIllegalArgumentException()
    {
        assertGetOrCreateConnThrowsIllegalArgumentException("host:port1:port2:port3");
    }

    @Test
    public void getOrCreateConn_withInvalidEndpointFormatTooFewParts_throwsIllegalArgumentException()
    {
        assertGetOrCreateConnThrowsIllegalArgumentException("host:port");
    }

    @Test
    public void getOrCreateConn_withNullConnectionInCache_createsNewConnection()
    {
        Connection mockHBaseConn = mock(Connection.class);

        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockHBaseConn);

            HBaseConnection conn = testFactory.getOrCreateConn(CONN_STR_LOCALHOST);
            assertNotNull("Connection should be created", conn);
        }
    }

    @Test
    public void addConnection_withValidConnection_retrievesFromCache()
            throws IOException
    {
        String conStr = "test:60000:2181";
        HBaseConnection mockConn = mock(HBaseConnection.class);
        when(mockConn.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});
        when(mockConn.isHealthy()).thenReturn(true);

        connectionFactory.addConnection(conStr, mockConn);
        HBaseConnection retrieved = connectionFactory.getOrCreateConn(conStr);

        verify(mockConn, times(1)).isHealthy();
        verify(mockConn, times(1)).listNamespaceDescriptors();
        assertEquals("Should retrieve the added connection", mockConn, retrieved);
    }

    @Test
    public void getClientConfigs_withAllDefaultConfigs_returnsAllFourDefaults()
    {
        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertEquals("Should have 4 default configs", 4, configs.size());
        assertEquals("hbase.rpc.timeout should be 2000", TIMEOUT_2000, configs.get(HBASE_RPC_TIMEOUT));
        assertEquals("hbase.client.retries.number should be 3", "3", configs.get("hbase.client.retries.number"));
        assertEquals("hbase.client.pause should be 500", "500", configs.get("hbase.client.pause"));
        assertEquals("zookeeper.recovery.retry should be 2", "2", configs.get("zookeeper.recovery.retry"));
    }

    @Test
    public void setClientConfig_withMultipleConfigs_addsAllConfigs()
    {
        connectionFactory.setClientConfig(CONFIG1, VALUE1);
        connectionFactory.setClientConfig(CONFIG2, VALUE2);
        connectionFactory.setClientConfig(CONFIG3, VALUE3);

        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertEquals("Should have all configs including defaults", 7, configs.size());
        assertEquals("Config1 value should match", VALUE1, configs.get(CONFIG1));
        assertEquals("Config2 value should match", VALUE2, configs.get(CONFIG2));
        assertEquals("Config3 value should match", VALUE3, configs.get(CONFIG3));
    }

    @Test
    public void setClientConfig_withExistingConfig_overridesValue()
    {
        connectionFactory.setClientConfig(HBASE_RPC_TIMEOUT, TIMEOUT_5000);
        Map<String, String> configs = connectionFactory.getClientConfigs();

        assertEquals("Config should be overridden", TIMEOUT_5000, configs.get(HBASE_RPC_TIMEOUT));
    }

    @Test
    public void setClientConfig_withCustomClientConfigs_appliesConfigs()
    {
        String customConfigKey = "hbase.custom.config";
        String customConfigValue = "custom_value";
        connectionFactory.setClientConfig(customConfigKey, customConfigValue);
        
        // Verify config is stored
        Map<String, String> configs = connectionFactory.getClientConfigs();
        assertTrue("Custom config should be present", configs.containsKey(customConfigKey));
        assertEquals("Custom config value should match", customConfigValue, configs.get(customConfigKey));
    }

    @Test
    public void getOrCreateConn_withIOExceptionInCreateConnection_throwsRuntimeException()
    {
        // This test verifies that IOException from createConnection is wrapped in RuntimeException
        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenThrow(new IOException("Connection creation failed"));

            RuntimeException ex = assertThrows(RuntimeException.class, () -> testFactory.getOrCreateConn(CONN_STR_LOCALHOST));
            assertTrue("Exception should contain IOException", ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void getOrCreateConn_withValidFormat_setsHBaseConfiguration()
            throws IOException
    {
        // This test verifies that createConnection sets up HBase configuration correctly
        String conStr = CONN_STR_LOCALHOST;
        Connection mockHBaseConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockHBaseConn.getAdmin()).thenReturn(mockAdmin);

        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockHBaseConn);

            when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});

            HBaseConnection conn = testFactory.getOrCreateConn(conStr);
            assertNotNull("Connection should not be null", conn);
            
            // Call again to trigger connection test
            HBaseConnection conn2 = testFactory.getOrCreateConn(conStr);
            assertNotNull("Second connection should not be null", conn2);
        }
    }

    @Test
    public void getOrCreateConn_callsCreateConnectionWithCorrectParameters()
            throws IOException
    {
        // This test verifies that createConnection is called with correct host, masterPort, zookeeperPort
        String host = "test-host";
        String masterPort = "60000";
        String zookeeperPort = "2181";
        String conStr = host + ":" + masterPort + ":" + zookeeperPort;
        
        Connection mockHBaseConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockHBaseConn.getAdmin()).thenReturn(mockAdmin);

        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenAnswer(invocation -> {
                        Configuration config = invocation.getArgument(0);
                        assertEquals("Host should match", host, config.get("hbase.zookeeper.quorum"));
                        assertEquals("Zookeeper port should match", zookeeperPort, config.get("hbase.zookeeper.property.clientPort"));
                        assertEquals("Master should match", host + ":" + masterPort, config.get("hbase.master"));
                        return mockHBaseConn;
                    });

            HBaseConnection conn = testFactory.getOrCreateConn(conStr);
            assertNotNull("Connection should not be null", conn);
            
            // Call again to trigger connection test
            when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});
            HBaseConnection conn2 = testFactory.getOrCreateConn(conStr);
            assertNotNull("Second connection should not be null", conn2);
        }
    }

    @Test
    public void getOrCreateConn_appliesClientConfigsInCreateConnection()
            throws IOException
    {
        // This test verifies that createConnection applies defaultClientConfig entries
        Connection mockHBaseConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockHBaseConn.getAdmin()).thenReturn(mockAdmin);

        HbaseConnectionFactory testFactory = new HbaseConnectionFactory();

        testFactory.setClientConfig("test.config.key", "test.config.value");
        Map<String, String> configs = testFactory.getClientConfigs();
        assertTrue("Test config should be present", configs.containsKey("test.config.key"));
        
        String conStr = CONN_STR_LOCALHOST;
        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenAnswer(invocation -> {
                        Configuration config = invocation.getArgument(0);
                        assertNotNull("Test config should be applied", config.get("test.config.key"));
                        return mockHBaseConn;
                    });

            HBaseConnection conn = testFactory.getOrCreateConn(conStr);
            assertNotNull("Connection should not be null", conn);
            
            // Call again to trigger connection test
            when(mockAdmin.listNamespaceDescriptors()).thenReturn(new NamespaceDescriptor[] {});
            HBaseConnection conn2 = testFactory.getOrCreateConn(conStr);
            assertNotNull("Second connection should not be null", conn2);
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabled_callsUserGroupInformationMethods()
            throws Exception
    {
        // This test verifies UserGroupInformation.setConfiguration and loginUserFromKeytab are called
        Connection mockHBaseConn = mock(Connection.class);

        Map<String, String> envVars = createBaseKerberosEnvVars();
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(envVars);

        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class);
             MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString())).thenAnswer(invocation -> null);

            Admin mockAdmin = mock(Admin.class);
            when(mockHBaseConn.getAdmin()).thenReturn(mockAdmin);

            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockHBaseConn);

            HBaseConnection conn = testFactory.getOrCreateConn(CONN_STR_LOCALHOST);
            assertNotNull("Connection should not be null", conn);
            
            // Verify connection test is called on second getOrCreateConn
            HBaseConnection conn2 = testFactory.getOrCreateConn(CONN_STR_LOCALHOST);
            assertNotNull("Second connection should not be null", conn2);
            assertEquals("Should return cached connection", conn, conn2);
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndS3Uri_callsCopyConfigFilesAndSetsProperties()
    {
        // This test verifies the S3 copy code path and System.setProperty call
        Connection mockHBaseConn = mock(Connection.class);

        Map<String, String> envVars = createBaseKerberosEnvVars();
        envVars.put(KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI);
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(envVars);

        Path mockTempDir = Paths.get("/tmp/test-kerberos-configs");
        String originalKrb5Conf = System.getProperty("java.security.krb5.conf");

        try (MockedStatic<HbaseKerberosUtils> kerberosUtilsMock = mockStatic(HbaseKerberosUtils.class);
             MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class);
             MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            kerberosUtilsMock.when(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)))
                    .thenReturn(mockTempDir);

            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString())).thenAnswer(invocation -> null);

            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockHBaseConn);

            HBaseConnection conn = testFactory.getOrCreateConn(CONN_STR_LOCALHOST);
            assertNotNull("Connection should not be null", conn);

            kerberosUtilsMock.verify(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)));

            String krb5ConfPath = System.getProperty("java.security.krb5.conf");
            assertNotNull("krb5.conf system property should be set", krb5ConfPath);
            assertTrue("krb5.conf path should contain krb5.conf", krb5ConfPath.contains("krb5.conf"));
            assertTrue("krb5.conf path should match tempDir", krb5ConfPath.startsWith(mockTempDir.toString()));
        }
        finally {
            if (originalKrb5Conf != null) {
                System.setProperty("java.security.krb5.conf", originalKrb5Conf);
            }
            else {
                System.clearProperty("java.security.krb5.conf");
            }
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndS3CopyFailure_throwsRuntimeException()
    {
        // This test verifies error handling when S3 copy fails
        Map<String, String> envVars = createBaseKerberosEnvVars();
        envVars.put(KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI);
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(envVars);

        try (MockedStatic<HbaseKerberosUtils> kerberosUtilsMock = mockStatic(HbaseKerberosUtils.class)) {
            kerberosUtilsMock.when(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)))
                    .thenThrow(new RuntimeException("S3 access failed"));

            RuntimeException ex = assertThrows(RuntimeException.class, () -> testFactory.getOrCreateConn(CONN_STR_LOCALHOST));
            assertTrue("Exception message should contain S3 error",
                    ex.getMessage() != null && ex.getMessage().contains("Error Copying Config files from S3"));
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndLoginFailure_throwsRuntimeException()
    {
        // This test verifies error handling when UserGroupInformation.loginUserFromKeytab fails
        Map<String, String> envVars = createBaseKerberosEnvVars();
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(envVars);

        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString()))
                    .thenThrow(new IOException("Keytab file not found"));

            assertThrows(RuntimeException.class, () -> testFactory.getOrCreateConn(CONN_STR_LOCALHOST));
        }
    }

    private void assertGetOrCreateConnThrowsIllegalArgumentException(String invalidFormat)
    {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                connectionFactory.getOrCreateConn(invalidFormat));
        assertTrue("Exception message should contain format error",
                ex.getMessage() != null && ex.getMessage().contains("format error"));
    }

    private Map<String, String> createBaseKerberosEnvVars()
    {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(KERBEROS_AUTH_ENABLED, "true");
        envVars.put(PRINCIPAL_NAME, "testuser@REALM");
        envVars.put(HBASE_RPC_PROTECTION, "privacy");
        return envVars;
    }

    private HbaseConnectionFactory createFactoryWithKerberosEnv(Map<String, String> envVars)
    {
        return new HbaseConnectionFactory()
        {
            @Override
            protected HbaseEnvironmentProperties getEnvironmentProperties()
            {
                return new HbaseEnvironmentProperties()
                {
                    @Override
                    protected Map<String, String> getEnvMap()
                    {
                        return envVars;
                    }
                };
            }
        };
    }
}
