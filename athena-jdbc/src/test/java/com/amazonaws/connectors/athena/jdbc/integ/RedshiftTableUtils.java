/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.connectors.athena.jdbc.integ;

import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.connectors.athena.jdbc.connection.DatabaseConnectionConfigBuilder;
import com.amazonaws.connectors.athena.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.connectors.athena.jdbc.connection.JdbcConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Create and populate the 'movies' DB table in a Redshift cluster.
 */
public class RedshiftTableUtils
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftTableUtils.class);

    private final String catalog;
    private final String tableName;
    private final Map properties;

    public RedshiftTableUtils(String catalog, String tableName, Map properties)
    {
        this.catalog = catalog;
        this.tableName = tableName;
        this.properties = properties;
    }

    /**
     * Creates the 'movies' DB table and inserts 2 rows.
     * @throws RuntimeException If an error is encountered during the create/insert queries.
     */
    public void setUpTable()
            throws RuntimeException
    {
        try (Connection connection = getDbConnection()) {
            // Prepare create table statement
            PreparedStatement createTable = connection.prepareStatement(String.format(
                    "create table %s(year int, title varchar, director varchar, lead varchar);", tableName));
            // Execute statement
            createTable.execute();
            logger.info("Created the 'movies' DB.");

            // Prepare insert values statements
            String insertString = "insert into " + tableName + " values(%s);";
            PreparedStatement insertValues = connection.prepareStatement(String.format(String.format(
                    "%s %s", insertString, insertString),
                    "2014, 'Interstellar', 'Christopher Nolan', 'Matthew McConaughey'",
                    "1986, 'Aliens', 'James Cameron', 'Sigourney Weaver'"));
            // Execute statement
            insertValues.execute();
            logger.info("Inserted 2 rows into the 'movies' DB.");
        }
        catch(SQLException e) {
            throw new RuntimeException("Unable to create table 'movies' in Redshift DB: " + e.getMessage(), e);
        }
    }

    /**
     * Gets a JDBC DB connection to the Redshift cluster.
     * @return Connection object.
     */
    private Connection getDbConnection()
    {
        DatabaseConnectionConfig connectionConfig = getDbConfig();
        JdbcConnectionFactory connectionFactory = new GenericJdbcConnectionFactory(connectionConfig, null);
        return connectionFactory.getConnection(null);
    }

    /**
     * Gets the DB connection configuration used to create a DB connection.
     * @return Connection Config object.
     * @throws RuntimeException If a configuration with the catalog (lambda function name) cannot be found in the
     * properties map.
     */
    private DatabaseConnectionConfig getDbConfig()
            throws RuntimeException
    {
        DatabaseConnectionConfigBuilder configBuilder = new DatabaseConnectionConfigBuilder();
        for (DatabaseConnectionConfig config : configBuilder.properties(properties).build()) {
            if (config.getCatalog().equals(catalog)) {
                return config;
            }
        }

        throw new RuntimeException("Unable to configure connection to Redshift DB.");
    }
}
