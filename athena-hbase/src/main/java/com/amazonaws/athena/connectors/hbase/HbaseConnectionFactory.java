package com.amazonaws.athena.connectors.hbase;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates and Caches HBase Connection Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be host:zookeeper_port:master_port
 */
public class HbaseConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnectionFactory.class);

    private final Map<String, Connection> clientCache = new HashMap<>();

    private final Map<String, String> defaultClientConfig = new HashMap<>();

    public HbaseConnectionFactory()
    {
        setClientConfig("hbase.rpc.timeout", "2000");
        setClientConfig("hbase.client.retries.number", "3");
        setClientConfig("hbase.client.pause", "500");
        setClientConfig("zookeeper.recovery.retry", "2");
    }

    public void setClientConfig(String name, String value)
    {
        defaultClientConfig.put(name, value);
    }

    public Map<String, String> getClientConfigs()
    {
        return Collections.unmodifiableMap(defaultClientConfig);
    }

    /**
     * Gets or Creates an HBase connection for the given connection string.
     *
     * @param conStr HBase connection details, format is expected to be host:zookeeper_port:master_port
     * @return An HBase connection if the connection succeeded, else the function will throw.
     */
    public Connection getOrCreateConn(String conStr)
    {
        logger.info("getOrCreateConn: enter[{}]", conStr);
        Connection conn = clientCache.get(conStr);

        if (conn == null || !connectionTest(conn)) {
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

    private Connection createConnection(String host, String masterPort, String zookeeperPort)
    {
        try {
            logger.info("createConnection: enter");
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", host);
            config.set("hbase.zookeeper.property.clientPort", zookeeperPort);
            config.set("hbase.master", host + ":" + masterPort);
            for (Map.Entry<String, String> nextConfig : defaultClientConfig.entrySet()) {
                logger.info("createConnection: applying client config {}:{}", nextConfig.getKey(), nextConfig.getValue());
                config.set(nextConfig.getKey(), nextConfig.getValue());
            }
            Connection conn = ConnectionFactory.createConnection(config);
            logger.info("createConnection: hbase.zookeeper.quorum:" + config.get("hbase.zookeeper.quorum"));
            logger.info("createConnection: hbase.zookeeper.property.clientPort:" + config.get("hbase.zookeeper.property.clientPort"));
            logger.info("createConnection: hbase.master:" + config.get("hbase.master"));
            return conn;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Runs a 'quick' test on the connection and then returns it if it passes.
     */
    private boolean connectionTest(Connection conn)
    {
        try {
            logger.info("connectionTest: Testing connection started.");
            conn.getAdmin().listTableNames();
            logger.info("connectionTest: Testing connection completed - success.");
            return true;
        }
        catch (RuntimeException | IOException ex) {
            logger.warn("getOrCreateConn: Exception while testing existing connection.", ex);
        }
        logger.info("connectionTest: Testing connection completed - fail.");
        return false;
    }

    @VisibleForTesting
    protected void addConnection(String conStr, Connection conn)
    {
        clientCache.put(conStr, conn);
    }
}
