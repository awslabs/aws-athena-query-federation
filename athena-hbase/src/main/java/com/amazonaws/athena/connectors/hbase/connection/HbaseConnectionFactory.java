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

import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.HBASE_RPC_PROTECTION;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_AUTH_ENABLED;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.PRINCIPAL_NAME;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder;

/**
 * Creates and Caches HBase Connection Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be host:master_port:zookeeper_port
 */
public class HbaseConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnectionFactory.class);

    private final int maxRetries = 3;

    private final Map<String, HBaseConnection> clientCache = new HashMap<>();

    private final Map<String, String> defaultClientConfig = new HashMap<>();

    public HbaseConnectionFactory()
    {
        setClientConfig("hbase.rpc.timeout", "2000");
        setClientConfig("hbase.client.retries.number", "3");
        setClientConfig("hbase.client.pause", "500");
        setClientConfig("zookeeper.recovery.retry", "2");
    }

    /**
     * Used to set HBase client config options that should be applied to all future connections.
     *
     * @param name The name of the property (e.g. hbase.rpc.timeout).
     * @param value The value of the property to set on the HBase client config object before construction.
     */
    public synchronized void setClientConfig(String name, String value)
    {
        defaultClientConfig.put(name, value);
    }

    /**
     * Provides access to the current HBase client config options used during connection construction.
     *
     * @return Map<String, String> where the Key is the config name and the value is the config value.
     * @note This can be helpful when logging diagnostic info.
     */
    public synchronized Map<String, String> getClientConfigs()
    {
        return Collections.unmodifiableMap(defaultClientConfig);
    }

    /**
     * Gets or Creates an HBase connection for the given connection string.
     *
     * @param conStr HBase connection details, format is expected to be host:master_port:zookeeper_port
     * @return An HBase connection if the connection succeeded, else the function will throw.
     */
    public synchronized HBaseConnection getOrCreateConn(String conStr)
    {
        logger.info("getOrCreateConn: enter");
        HBaseConnection conn = clientCache.get(conStr);

        if (conn == null || !connectionTest(conn)) {
            if (conn != null) {
                conn.close();
            }
            String[] endpointParts = conStr.split(":");
            if (endpointParts.length == 3) {
                conn = createConnection(endpointParts[0], endpointParts[1], endpointParts[2]);
                clientCache.put(conStr, conn);
            }
            else {
                throw new IllegalArgumentException("Hbase endpoint format error.");
            }
        }

        logger.info("getOrCreateConn: exit");
        return conn;
    }

    private HBaseConnection createConnection(String host, String masterPort, String zookeeperPort)
    {
        logger.info("createConnection: enter");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", host);
        config.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        config.set("hbase.master", host + ":" + masterPort);
        for (Map.Entry<String, String> nextConfig : defaultClientConfig.entrySet()) {
            logger.info("createConnection: applying client config {}:{}", nextConfig.getKey(), nextConfig.getValue());
            config.set(nextConfig.getKey(), nextConfig.getValue());
        }

        Map<String, String> configOptions = System.getenv();
        boolean kerberosAuthEnabled = configOptions.get(KERBEROS_AUTH_ENABLED) != null && "true".equalsIgnoreCase(configOptions.get(KERBEROS_AUTH_ENABLED));
        logger.info("Kerberos Authentication Enabled: " + kerberosAuthEnabled);
        if (kerberosAuthEnabled) {
            String keytabLocation = null;
            config.set("hbase.rpc.protection", configOptions.get(HBASE_RPC_PROTECTION));
            logger.info("hbase.rpc.protection: " + config.get("hbase.rpc.protection"));
            String s3uri = configOptions.get(KERBEROS_CONFIG_FILES_S3_REFERENCE);
            if (StringUtils.isNotBlank(s3uri)) {
                try {
                    Path tempDir = copyConfigFilesFromS3ToTempFolder(configOptions);
                    logger.debug("tempDir: " + tempDir);
                    keytabLocation = tempDir + File.separator + "hbase.keytab";
                    System.setProperty("java.security.krb5.conf", tempDir + File.separator + "krb5.conf");
                    logger.debug("keytabLocation: " + keytabLocation);
                }
                catch (Exception e) {
                    throw new RuntimeException("Error Copying Config files from S3 to temp folder: ", e);
                }
            }

            UserGroupInformation.setConfiguration(config);
            try {
                String principalName = configOptions.get(PRINCIPAL_NAME);
                UserGroupInformation.loginUserFromKeytab(principalName, keytabLocation);
            }
            catch (IOException ex) {
                throw new RuntimeException("Exception in UserGroupInformation.loginUserFromKeytab: ", ex);
            }
            logger.debug("UserGroupInformation.loginUserFromKeytab Success.");
        }
        return new HBaseConnection(config, maxRetries);
    }

    /**
     * Runs a 'quick' test on the connection and then returns it if it passes.
     */
    private boolean connectionTest(HBaseConnection conn)
    {
        try {
            logger.info("connectionTest: Testing connection started.");
            if (conn.isHealthy()) {
                conn.listNamespaceDescriptors();
                logger.info("connectionTest: Testing connection completed - success.");
                return true;
            }
        }
        catch (RuntimeException | IOException ex) {
            logger.warn("getOrCreateConn: Exception while testing existing connection.", ex);
        }
        logger.info("connectionTest: Testing connection completed - fail.");
        return false;
    }

    /**
     * Injects a connection into the client cache.
     *
     * @param conStr The connection string (aka the cache key)
     * @param conn The connection to inject into the client cache, most often a Mock used in testing.
     */
    @VisibleForTesting
    protected synchronized void addConnection(String conStr, HBaseConnection conn)
    {
        clientCache.put(conStr, conn);
    }
}
