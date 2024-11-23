/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates and Caches HBase Connection Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be like:
 *  mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=<certs.pem>&replicaSet=<replica_set>
 */
public class DocDBConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBConnectionFactory.class);
    private final Map<String, MongoClient> clientCache = new HashMap<>();

    /**
     * Used to get an existing, pooled, connection or to create a new connection
     * for the given connection string.
     *
     * @param connStr MongoClient connection details, format is expected to be:
     *                  mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=<certs.pem>&replicaSet=<replica_set>
     * @return A MongoClient connection if the connection succeeded, else the function will throw.
     */
    public synchronized MongoClient getOrCreateConn(String connStr)
    {
        logger.info("getOrCreateConn: enter");
        MongoClient result = clientCache.get(connStr);

        if (result == null || !connectionTest(result)) {
            //Setup SSL Trust Store:
            if (connStr.toLowerCase().contains("ssl=true")) {
                logger.info("MongoClient is using SSL; thus setting up System properties for trust store");
                System.setProperty("javax.net.ssl.trustStore", "rds-truststore.jks");
                System.setProperty("javax.net.ssl.trustStorePassword", "federationStorePass");
            }
            result = MongoClients.create(connStr);
            clientCache.put(connStr, result);
        }

        logger.info("getOrCreateConn: exit");
        return result;
    }

    /**
     * Runs a 'quick' test on the connection and then returns it if it passes.
     */
    private boolean connectionTest(MongoClient conn)
    {
        try {
            logger.info("connectionTest: Testing connection started.");
            conn.listDatabaseNames();
            logger.info("connectionTest: Testing connection completed - success.");
            return true;
        }
        catch (RuntimeException ex) {
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
    protected synchronized void addConnection(String conStr, MongoClient conn)
    {
        clientCache.put(conStr, conn);
    }
}
