/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * This class wraps Hbase's Connection construct and provides both a simplified facade for common operations
 * but also automatic retry and reconnect logic to make client reuse simpler. Some specific cases that this
 * construct handles are: replacement of an HBase region server, replacement of HBase master server, other
 * transient errors while communicating with HBase.
 */
public class HBaseConnection
{
    private static final Logger logger = LoggerFactory.getLogger(HBaseConnection.class);

    private final Configuration config;
    private final int maxRetries;
    private int retries = 0;
    private Connection connection;

    /**
     * Used by HBaseConnectionFactory to construct new connections.
     *
     * @param config The HBase config to use for this connection.
     * @param maxRetries The max retries this connection will allow before marking itself unhealthy.
     */
    protected HBaseConnection(Configuration config, int maxRetries)
    {
        this.config = config;
        this.maxRetries = maxRetries;
        this.connection = connect(config);
    }

    /**
     * Used to facilitate testing by allowing for injection of the underlying HBase connection.
     *
     * @param connection The actual Hbase connection to use.
     * @param maxRetries The max retries this connection will allow before marking itself unhealthy.
     */
    @VisibleForTesting
    protected HBaseConnection(Connection connection, int maxRetries)
    {
        this.config = null;
        this.maxRetries = maxRetries;
        this.connection = connection;
    }

    /**
     * Used to determine if this connection can be reused or if it is unhealthy.
     *
     * @return True if the connection can be safely reused, false otherwise.
     */
    public synchronized boolean isHealthy()
    {
        return retries < maxRetries;
    }

    /**
     * Lists the available namespaces in the HBase instance.
     *
     * @return Array of NamespaceDescriptor representing the available namespaces in the HBase instance.
     * @throws IOException
     */
    public NamespaceDescriptor[] listNamespaceDescriptors()
            throws IOException
    {
        return callWithReconnectAndRetry(() -> {
            Admin admin = getConnection().getAdmin();
            return admin.listNamespaceDescriptors();
        });
    }

    /**
     * Lists the table names present in the given schema (aka namespace)
     *
     * @param schemaName The schema name (aka hbase namespace) to list table names from.
     * @return An array of hbase TableName objects for the given schema name.
     */
    public TableName[] listTableNamesByNamespace(String schemaName)
    {
        return callWithReconnectAndRetry(() -> {
            Admin admin = getConnection().getAdmin();
            return admin.listTableNamesByNamespace(schemaName);
        });
    }

    /**
     * Retreieves the regions for the requested TableName.
     *
     * @param tableName The fully qualified HBase TableName for which we should obtain region info.
     * @return List of HRegionInfo representing the regions hosting the requested table.
     */
    public List<HRegionInfo> getTableRegions(TableName tableName)
    {
        return callWithReconnectAndRetry(() -> {
            Admin admin = getConnection().getAdmin();
            return admin.getTableRegions(tableName);
        });
    }

    /**
     * Used to perform a scan of the given table, scan, and resultProcessor.
     *
     * @param tableName The HBase table to scan.
     * @param scan The HBase scan (filters, etc...) to run.
     * @param resultProcessor The ResultProcessor that will be used to process the results of the scan.
     * @param <T> The return type of the ResultProcessor and this this method.
     * @return The result produced by the resultProcessor when it has completed processing all results of the scan.
     */
    public <T> T scanTable(TableName tableName, Scan scan, ResultProcessor<T> resultProcessor)
    {
        return callWithReconnectAndRetry(() -> {
            try (Table table = connection.getTable(tableName);
                    ResultScanner scanner = table.getScanner(scan)) {
                try {
                    return resultProcessor.scan(scanner);
                }
                catch (RuntimeException ex) {
                    throw new UnrecoverableException("Scanner threw exception - " + ex.getMessage(), ex);
                }
            }
        });
    }

    /**
     * Used to close this connection by closing the underlying HBase Connection.
     */
    protected void close()
    {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (IOException | RuntimeException ex) {
            logger.warn("close: Exception while closing connection.");
        }
    }

    @VisibleForTesting
    protected int getRetries()
    {
        return retries;
    }

    /**
     * Used to invoke operations with HBase reconnect and retry logic.
     *
     * @param callable The callable to invoke and monitor for errors.
     * @param <T> The return type of the callable.
     * @return The result of the callable, if it was successfull.
     * @note This retry logic needs to be adjusted for 'write' usecases.
     */
    private <T> T callWithReconnectAndRetry(Callable<T> callable)
    {
        while (isHealthy()) {
            try {
                return callable.call();
            }
            catch (RuntimeException ex) {
                logger.warn("Exception while calling hbase, will attempt to reconnect and retry.", ex);
                reconnect();
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
            catch (UnrecoverableException ex) {
                throw new RuntimeException(ex);
            }
            catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        throw new RuntimeException("Exhausted hbase retries...");
    }

    /**
     * Creates a new HBase connection using the provided config.
     *
     * @param config The config to use for the new HBase connection.
     * @return The new connection.
     */
    @VisibleForTesting
    protected synchronized Connection connect(Configuration config)
    {
        try {
            Connection conn = ConnectionFactory.createConnection(config);
            logger.info("connect: hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"));
            logger.info("connect: hbase.zookeeper.property.clientPort:" + config.get("hbase.zookeeper.property.clientPort"));
            logger.info("connect: hbase.master:" + config.get("hbase.master"));
            return conn;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Used to reconnect to HBase by freeing existing connection resources, incrementing retry metrics, and ultimately
     * creating the new connection.
     *
     * @return The new connection.
     */
    private synchronized Connection reconnect()
    {
        retries++;
        close();

        try {
            connection = connect(config);
        }
        catch (RuntimeException ex) {
            logger.warn("reConnect: Exception while reconnecting, marking connection as unhealthy and throwing.", ex);
            retries = maxRetries;
            throw ex;
        }

        return connection;
    }

    /**
     * Provides access to the current connection if present and healthy.
     *
     * @return The underlying HBase connection.
     */
    private synchronized Connection getConnection()
    {
        if (connection == null || !isHealthy()) {
            throw new RuntimeException("Invalid connection");
        }
        return connection;
    }
}
