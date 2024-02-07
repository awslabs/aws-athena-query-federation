/*-
 * #%L
 * athena-vertica
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
package com.amazonaws.athena.connectors.vertica;

import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates and Caches JDBC Connection Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be like:
 *  jdbc:vertica://<HOSTNAME></>>:5433/VerticaDB?user&password>
 */

public class VerticaConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaConnectionFactory.class);
    private final Map<String, Connection> clientCache = new HashMap<>();
    private final int CONNECTION_TIMEOUT = 60;

    /**
     * Used to get an existing, pooled, connection or to create a new connection
     * for the given connection string.
     *
     * @param connStr Vertica connection details
     * @return A JDBC connection if the connection succeeded, else the function will throw.
     */

    public synchronized Connection getOrCreateConn(String connStr) {
        Connection result = null;
        try {
            Class.forName("com.vertica.jdbc.Driver").newInstance();
            result = clientCache.get(connStr);

        if (result == null || !connectionTest(result)) {
            logger.info("Unable to reuse an existing connection, creating a new one.");
            result = DriverManager.getConnection(connStr);
            clientCache.put(connStr, result);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error in initializing Vertica JDBC Driver: " + e.getMessage(), e);
        }

        return result;
    }

    /**
     * Runs a 'quick' test on the connection and then returns it if it passes.
     */
    private boolean connectionTest(Connection conn) throws SQLException {
        try {
            return conn.isValid(CONNECTION_TIMEOUT);
        }
        catch (RuntimeException | SQLException ex) {
            logger.warn("getOrCreateConn: Exception while testing existing connection.", ex);
        }
        return false;
    }

    /**
     * Injects a connection into the client cache.
     *
     * @param conStr The connection string (aka the cache key)
     * @param conn The connection to inject into the client cache, most often a Mock used in testing.
     */
    @VisibleForTesting
    protected synchronized void addConnection(String conStr, Connection conn)
    {
        clientCache.put(conStr, conn);
    }

}
