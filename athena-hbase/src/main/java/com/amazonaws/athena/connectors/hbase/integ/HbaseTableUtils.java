/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.integ;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.HBASE_RPC_PROTECTION;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_AUTH_ENABLED;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.KERBEROS_CONFIG_FILES_S3_REFERENCE;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.PRINCIPAL_NAME;
import static com.amazonaws.athena.connectors.hbase.HbaseKerberosUtils.copyConfigFilesFromS3ToTempFolder;

/**
 * This class can be used to establish a connection to a HBase instance. Once the connection is established, a new
 * database/namespace and table can be created, and new rows can be inserted into the newly created table.
 */
public class HbaseTableUtils
        implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseTableUtils.class);

    private final NamespaceDescriptor hbaseNamespaceDescriptor;
    private final HTableDescriptor hbaseTableDescriptor;
    private final Connection hbaseClient;

    /**
     * The constructor establishes a connection to the HBase instance.
     * @param databaseName Name of the database or namespace.
     * @param tableName Name of the database table.
     * @param connectionStr Connection string used to connect to the HBase instance
     *                      (e.g. ec2-000-000-000-000.compute-1.amazonaws.com:50075:2081)
     * @throws IOException An error was encountered while trying to connect to the HBase instance.
     */
    public HbaseTableUtils(String databaseName, String tableName, String connectionStr)
            throws IOException
    {
        hbaseNamespaceDescriptor = NamespaceDescriptor.create(databaseName).build();
        hbaseTableDescriptor = new HTableDescriptor(TableName.valueOf(databaseName.getBytes(), tableName.getBytes()));
        hbaseClient = ConnectionFactory.createConnection(getHbaseConfiguration(connectionStr));
    }

    /**
     * Creates a configuration object used to connect to the HBase instance.
     * @param connectionStr Connection string used to connect to the HBase instance
     *                      (e.g. ec2-000-000-000-000.compute-1.amazonaws.com:50075:2081)
     * @return A Configuration object.
     */
    private Configuration getHbaseConfiguration(String connectionStr)
    {
        Configuration configuration = HBaseConfiguration.create();
        // Using the above example, hostName = ec2-000-000-000-000.compute-1.amazonaws.com
        String hostName = connectionStr.substring(0, connectionStr.indexOf(':'));
        // Using the above example, masterHost = ec2-000-000-000-000.compute-1.amazonaws.com:50075
        String masterHost = connectionStr.substring(0, connectionStr.lastIndexOf(':'));
        // Using the above example, zookeeperPort = 2081
        String zookeeperPort = connectionStr.substring(connectionStr.lastIndexOf(':') + 1);
        configuration.set("hbase.zookeeper.quorum", hostName);
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        configuration.set("hbase.master", masterHost);
        configuration.set("hbase.rpc.timeout", "2000");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.client.pause", "500");
        configuration.set("zookeeper.recovery.retry", "2");

        java.util.Map<String, String> configOptions = System.getenv();
        boolean kerberosAuthEnabled = configOptions.get(KERBEROS_AUTH_ENABLED) != null && "true".equalsIgnoreCase(configOptions.get(KERBEROS_AUTH_ENABLED));
        logger.info("Kerberos Authentication Enabled: " + kerberosAuthEnabled);
        if (kerberosAuthEnabled) {
            String keytabLocation = null;
            configuration.set("hbase.rpc.protection", configOptions.get(HBASE_RPC_PROTECTION));
            logger.info("hbase.rpc.protection: " + configuration.get("hbase.rpc.protection"));
            String s3uri = configOptions.get(KERBEROS_CONFIG_FILES_S3_REFERENCE);
            if (StringUtils.isNotBlank(s3uri)) {
                try {
                    Path tempDir = copyConfigFilesFromS3ToTempFolder(configOptions);
                    logger.debug("tempDir: " + tempDir);
                    keytabLocation = tempDir + File.separator + "hbase.keytab";
                    System.setProperty("java.security.krb5.conf", tempDir + File.separator + "krb5.conf");
                    logger.debug("krb5.conf location: " + tempDir + File.separator + "krb5.conf");
                }
                catch (Exception e) {
                    throw new RuntimeException("Error Copying Config files from S3 to temp folder: ", e);
                }
            }
            logger.debug("keytabLocation: " + keytabLocation);

            UserGroupInformation.setConfiguration(configuration);
            try {
                String principalName = configOptions.get(PRINCIPAL_NAME);
                UserGroupInformation.loginUserFromKeytab(principalName, keytabLocation);
            }
            catch (IOException ex) {
                throw new RuntimeException("Exception in UserGroupInformation.loginUserFromKeytab: ", ex);
            }
            logger.debug("UserGroupInformation.loginUserFromKeytab Success.");
        }

        return configuration;
    }

    /**
     * Creates the database/namespace and table in the HBase instance.
     * @param familyColumns A list of family column names.
     * @throws IOException An error was encountered while creating the database or table.
     */
    public void createDbAndTable(List<String> familyColumns)
            throws IOException
    {
        try (Admin hbaseAdmin = hbaseClient.getAdmin()) {
            // Create the HBase database/namespace
            hbaseAdmin.createNamespace(hbaseNamespaceDescriptor);
            logger.info("Created Namespace: {}", hbaseNamespaceDescriptor.getName());
            // Insert family columns into table
            familyColumns.forEach(familyColumn -> hbaseTableDescriptor.addFamily(new HColumnDescriptor(familyColumn)));
            // Create table in Hbase database/namespace
            hbaseAdmin.createTable(hbaseTableDescriptor);
            logger.info("Created Table: {}", hbaseTableDescriptor.getTableName().getNameAsString());
        }
    }

    /**
     * Inserts rows into the newly created database table.
     * @param rows A list of Put (row) objects containing details such as the family name, column name, and value.
     * @throws IOException An error was encountered trying to insert rows into the table.
     */
    public void insertRows(List<Put> rows)
            throws IOException
    {
        try (Table table = hbaseClient.getTable(hbaseTableDescriptor.getTableName())) {
            table.put(rows);
            logger.info("New rows inserted successfully.");
        }
    }

    /**
     * Closes the connection to the HBase instance.
     * @throws IOException
     */
    @Override
    public void close()
            throws IOException
    {
        hbaseClient.close();
    }
}
