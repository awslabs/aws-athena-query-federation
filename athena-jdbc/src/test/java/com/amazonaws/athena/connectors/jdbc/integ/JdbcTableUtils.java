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
package com.amazonaws.athena.connectors.jdbc.integ;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfigBuilder;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Facilitates the creation and management of a DB table using JDBC.
 */
public class JdbcTableUtils
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcTableUtils.class);

    private final String catalog;
    private final String schemaName;
    private final String tableName;
    private final Map environmentVars;
    private final Map properties;

    public JdbcTableUtils(String catalog, TableName table, Map environmentVars)
    {
        this(catalog, table, environmentVars, null);
    }

    public JdbcTableUtils(String catalog, TableName table, Map environmentVars, Map properties)
    {
        this.catalog = catalog;
        this.schemaName = table.getSchemaName();
        this.tableName = table.getTableName();
        this.environmentVars = environmentVars;
        this.properties = properties;
    }

    /**
     * Creates a DB schema.
     * @throws RuntimeException The SQL statement failed.
     */
    public void createDbSchema()
            throws RuntimeException
    {
        try (Connection connection = getDbConnection()) {
            // Prepare create schema statement
            String createStatement = String.format("create schema %s;", schemaName);
            PreparedStatement createSchema = connection.prepareStatement(createStatement);
            logger.info("Statement prepared: {}", createStatement);
            // Execute statement
            createSchema.execute();
            logger.info("Created the DB schema: {}", schemaName);
        }
        catch(SQLException e) {
            throw new RuntimeException(String.format("Unable to create DB schema (%s): %s",
                    schemaName, e.getMessage()), e);
        }
    }

    /**
     * Creates a DB table.
     * @param tableSchema String representing the table's schema (e.g. "year int, first_name varchar").
     * @throws RuntimeException The SQL statement failed.
     */
    public void createTable(String tableSchema)
            throws RuntimeException
    {
        try (Connection connection = getDbConnection()) {
            // Prepare create table statement
            String createStatement = String.format("create table %s.%s (%s);", schemaName, tableName, tableSchema);
            PreparedStatement createTable = connection.prepareStatement(createStatement);
            logger.info("Statement prepared: {}", createStatement);
            // Execute statement
            createTable.execute();
            logger.info("Created the '{}' table.", tableName);
        }
        catch(SQLException e) {
            throw new RuntimeException(String.format("Unable to create table '%s' in DB '%s': %s",
                    tableName, schemaName, e.getMessage()), e);
        }
    }

    /**
     * Inserts a row into a DB table.
     * @param tableValues String representing the row's values (e.g. "1992, 'James'").
     * @throws RuntimeException The SQL statement failed.
     */
    public void insertRow(String tableValues)
            throws RuntimeException
    {
        try (Connection connection = getDbConnection()) {

            // Prepare insert values statements
            String insertStatement = String.format("insert into %s.%s values(%s);", schemaName, tableName, tableValues);
            PreparedStatement insertValues = connection.prepareStatement(insertStatement);
            logger.info("Statement prepared: {}", insertStatement);
            // Execute statement
            insertValues.execute();
            logger.info("Inserted row into the '{}' table.", tableName);
        }
        catch(SQLException e) {
            throw new RuntimeException(String.format("Unable to insert row into table '%s': %s",
                    tableName, e.getMessage()), e);
        }
    }

    /**
     * Gets a JDBC DB connection.
     * @return Connection object.
     */
    protected Connection getDbConnection()
    {
        DatabaseConnectionConfig connectionConfig = getDbConfig();
        JdbcConnectionFactory connectionFactory = new GenericJdbcConnectionFactory(connectionConfig, properties);
        return connectionFactory.getConnection(null);
    }

    /**
     * Gets the DB connection configuration used to create a DB connection.
     * @return Connection Config object.
     * @throws RuntimeException If a configuration with the catalog (lambda function name) cannot be found in the
     * environmentVars map.
     */
    protected DatabaseConnectionConfig getDbConfig()
            throws RuntimeException
    {
        DatabaseConnectionConfigBuilder configBuilder = new DatabaseConnectionConfigBuilder();
        for (DatabaseConnectionConfig config : configBuilder.properties(environmentVars).build()) {
            if (config.getCatalog().equals(catalog)) {
                return config;
            }
        }

        throw new RuntimeException("Unable to configure connection to DB.");
    }
}
