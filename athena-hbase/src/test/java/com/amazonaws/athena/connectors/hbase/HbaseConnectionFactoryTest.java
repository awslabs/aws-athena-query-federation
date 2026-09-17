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
package com.amazonaws.athena.connectors.hbase;

import org.apache.hadoop.conf.Configuration;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
    private static final String CONN_STR = "conStr";
    private static final String VALID_CONN_STR = "localhost:60000:2181";
    private static final String TEST_USER = "testuser@REALM";
    private static final String TEST_RPC_PROTECTION = "privacy";
    private static final String TEST_S3_URI = "s3://test-bucket/test-prefix/";

    private HbaseConnectionFactory connectionFactory;

    @Before
    public void setUp()
            throws Exception
    {
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
        Connection mockConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockConn.getAdmin()).thenReturn(mockAdmin);

        connectionFactory.addConnection(CONN_STR, mockConn);
        Connection conn = connectionFactory.getOrCreateConn(CONN_STR);

        assertEquals("Should return cached connection", mockConn, conn);
        verify(mockConn, times(1)).getAdmin();
        verify(mockAdmin, times(1)).listTableNames();
    }

    @Test
    public void getOrCreateConn_withInvalidEndpointFormat_throwsIllegalArgumentException()
    {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                connectionFactory.getOrCreateConn("invalid:format"));
        assertTrue("Exception message should contain format error", ex.getMessage().contains("format error"));
    }

    @Test
    public void getOrCreateConn_withUnhealthyConnection_replacesConnection()
            throws IOException
    {
        Connection mockConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockConn.getAdmin()).thenReturn(mockAdmin);
        when(mockAdmin.listTableNames()).thenThrow(new RuntimeException("Connection test failed"));

        connectionFactory.addConnection(VALID_CONN_STR, mockConn);
        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            Connection newMockConn = mock(Connection.class);
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(newMockConn);
            connectionFactory.getOrCreateConn(VALID_CONN_STR);
            verify(mockConn, times(1)).getAdmin();
        }
    }

    @Test
    public void getOrCreateConn_withValidFormat_callsCreateConnection()
            throws IOException
    {
        // This test verifies that createConnection is called through getOrCreateConn
        Connection mockConn = mock(Connection.class);
        Admin mockAdmin = mock(Admin.class);
        when(mockConn.getAdmin()).thenReturn(mockAdmin);

        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockConn);

            Connection conn = connectionFactory.getOrCreateConn(VALID_CONN_STR);
            assertNotNull("Connection should not be null", conn);
            assertEquals("Connection should match mock", mockConn, conn);

            // Verify it's cached - second call should return same connection without creating new one
            Connection conn2 = connectionFactory.getOrCreateConn(VALID_CONN_STR);
            assertNotNull("Second connection should not be null", conn2);
            assertEquals("Second connection should be cached (same instance)", conn, conn2);
            // Verify createConnection was only called once, proving cache was used
            connectionFactoryMock.verify(() -> ConnectionFactory.createConnection(any(Configuration.class)), times(1));
        }
    }

    @Test
    public void setClientConfig_withCustomConfig_appliesConfigs()
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
        try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenThrow(new IOException("Connection creation failed"));

            RuntimeException ex = assertThrows(RuntimeException.class, () -> connectionFactory.getOrCreateConn(VALID_CONN_STR));
            assertTrue("Exception should contain IOException", ex.getCause() instanceof IOException);
        }
    }

    @Test
    public void getClientConfigs_withDefaults_returnsAllDefaultConfigs()
    {
        Map<String, String> configs = connectionFactory.getClientConfigs();
        assertTrue("Should have hbase.rpc.timeout", configs.containsKey("hbase.rpc.timeout"));
        assertTrue("Should have hbase.client.retries.number", configs.containsKey("hbase.client.retries.number"));
        assertTrue("Should have hbase.client.pause", configs.containsKey("hbase.client.pause"));
        assertTrue("Should have zookeeper.recovery.retry", configs.containsKey("zookeeper.recovery.retry"));
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndS3Uri_callsCopyConfigFilesAndSetsProperties()
    {
        // This test verifies the S3 copy code path and System.setProperty call
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(createBaseKerberosEnvVarsWithS3Ref());

        Path mockTempDir = Paths.get("/tmp/test-kerberos-configs");
        String originalKrb5Conf = System.getProperty("java.security.krb5.conf");

        try (MockedStatic<HbaseKerberosUtils> kerberosUtilsMock = mockStatic(HbaseKerberosUtils.class);
             MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            // Mock copyConfigFilesFromS3ToTempFolder to return a temp path
            kerberosUtilsMock.when(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)))
                    .thenReturn(mockTempDir);

            // Mock UserGroupInformation methods
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString())).thenAnswer(invocation -> null);

            Connection mockConn = mock(Connection.class);

            try (MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
                connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                        .thenReturn(mockConn);

                Connection conn = testFactory.getOrCreateConn(VALID_CONN_STR);
                assertNotNull("Connection should not be null", conn);
                assertEquals("Connection should match mock", mockConn, conn);
            }

            // Verify copyConfigFilesFromS3ToTempFolder was called
            kerberosUtilsMock.verify(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)));

            // Verify System.setProperty was called for krb5.conf
            String krb5ConfPath = System.getProperty("java.security.krb5.conf");
            assertNotNull("krb5.conf system property should be set", krb5ConfPath);
            assertTrue("krb5.conf path should contain krb5.conf", krb5ConfPath.contains("krb5.conf"));
            assertTrue("krb5.conf path should match tempDir", krb5ConfPath.startsWith(mockTempDir.toString()));
        }
        finally {
            // Restore original property
            if (originalKrb5Conf != null) {
                System.setProperty("java.security.krb5.conf", originalKrb5Conf);
            }
            else {
                System.clearProperty("java.security.krb5.conf");
            }
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabled_callsUserGroupInformationMethods()
    {
        // This test verifies UserGroupInformation.setConfiguration and loginUserFromKeytab are called
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(createBaseKerberosEnvVars());

        Connection mockConn = mock(Connection.class);

        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class);
             MockedStatic<ConnectionFactory> connectionFactoryMock = mockStatic(ConnectionFactory.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString())).thenAnswer(invocation -> null);

            connectionFactoryMock.when(() -> ConnectionFactory.createConnection(any(Configuration.class)))
                    .thenReturn(mockConn);

            Connection conn = testFactory.getOrCreateConn(VALID_CONN_STR);
            assertNotNull("Connection should not be null", conn);
            assertEquals("Connection should match mock", mockConn, conn);
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndS3CopyFailure_throwsRuntimeException()
    {
        // This test verifies error handling when S3 copy fails
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(createBaseKerberosEnvVarsWithS3Ref());

        try (MockedStatic<HbaseKerberosUtils> kerberosUtilsMock = mockStatic(HbaseKerberosUtils.class)) {
            // Mock copyConfigFilesFromS3ToTempFolder to throw exception
            kerberosUtilsMock.when(() -> HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder(any(Map.class)))
                    .thenThrow(new RuntimeException("S3 access failed"));

            RuntimeException ex = assertThrows(RuntimeException.class, () -> testFactory.getOrCreateConn(VALID_CONN_STR));
            assertTrue("Exception message should contain S3 error",
                    ex.getMessage() != null && ex.getMessage().contains("Error Copying Config files from S3"));
        }
    }

    @Test
    public void getOrCreateConn_withKerberosEnabledAndLoginFailure_throwsRuntimeException()
    {
        // This test verifies error handling when UserGroupInformation.loginUserFromKeytab fails
        HbaseConnectionFactory testFactory = createFactoryWithKerberosEnv(createBaseKerberosEnvVars());

        try (MockedStatic<UserGroupInformation> ugiMock = mockStatic(UserGroupInformation.class)) {
            ugiMock.when(() -> UserGroupInformation.setConfiguration(any(Configuration.class))).thenAnswer(invocation -> null);
            ugiMock.when(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString()))
                    .thenThrow(new IOException("Keytab file not found"));

            assertThrows(RuntimeException.class, () -> testFactory.getOrCreateConn(VALID_CONN_STR));
        }
    }

    /**
     * Creates base Kerberos env vars (no S3 reference).
     */
    private Map<String, String> createBaseKerberosEnvVars()
    {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(KERBEROS_AUTH_ENABLED, "true");
        envVars.put(PRINCIPAL_NAME, TEST_USER);
        envVars.put(HBASE_RPC_PROTECTION, TEST_RPC_PROTECTION);
        return envVars;
    }

    /**
     * Creates base Kerberos env vars including S3 config reference.
     */
    private Map<String, String> createBaseKerberosEnvVarsWithS3Ref()
    {
        Map<String, String> envVars = createBaseKerberosEnvVars();
        envVars.put(KERBEROS_CONFIG_FILES_S3_REFERENCE, TEST_S3_URI);
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
