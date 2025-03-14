/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

public class SqlServerCaseInsensitiveResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerCaseInsensitiveResolver.class);
    private static final String OBJECT_NAME_QUERY_TEMPLATE = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE LOWER(TABLE_SCHEMA) = ? AND LOWER(TABLE_NAME) = ?";
    private static final String CASING_MODE = "casing_mode";
    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(SCHEMA_NAME) = ?";
    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMA_NAME";

    private SqlServerCaseInsensitiveResolver()
    {
    }

    private enum SqlserverCasingMode
    {
        NONE,
        CASE_INSENSITIVE_SEARCH
    }

    public static String getAdjustedSchemaNameBasedOnConfig(Connection connection, String schemaName, Map<String, String> configOptions)
    {
        SqlserverCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case CASE_INSENSITIVE_SEARCH:
                LOGGER.info("casing mode is `CASE_INSENSITIVE_SEARCH`: adjusting casing from Sql Server case insensitive search for Schema...");
                return getSchemaNameCaseInsensitively(connection, schemaName);
            case NONE:
                LOGGER.info("casing mode is `NONE`: not adjust casing from input for Schema");
                return schemaName;
        }
        return schemaName;
    }

    public static String getSchemaNameCaseInsensitively(Connection connection, String schemaName)
    {
        String nameFromSqlServer = null;
        int i = 0;
        try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                .withConnection(connection)
                .withQuery(SCHEMA_NAME_QUERY_TEMPLATE)
                .withParameters(Arrays.asList(schemaName.toLowerCase())).build();
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                i++;
                String schemaNameCandidate = resultSet.getString(SCHEMA_NAME_COLUMN_KEY);
                LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", SCHEMA_NAME_COLUMN_KEY, schemaNameCandidate);
                nameFromSqlServer = schemaNameCandidate;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (i == 0 || i > 1) {
            throw new RuntimeException(String.format("Schema name case insensitive match failed, number of match : %d", i));
        }

        return nameFromSqlServer;
    }

    public static TableName getAdjustedTableObjectNameBasedOnConfig(final Connection connection, TableName tableName, Map<String, String> configOptions)
            throws SQLException
    {
        SqlserverCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case CASE_INSENSITIVE_SEARCH:
                TableName tableNameResult = getObjectNameCaseInsensitively(connection, tableName);
                LOGGER.info("casing mode is `CASE_INSENSITIVE_SEARCH`: adjusting casing from SqlServer case insensitive search for TableName object. TableName:{}", tableNameResult);
                return tableNameResult;
            case NONE:
                LOGGER.info("casing mode is `NONE`: not adjust casing from input for TableName object. TableName:{}", tableName);
                return tableName;
        }
        LOGGER.warn("casing mode is empty: not adjust casing from input for TableName object. TableName:{}", tableName);
        return tableName;
    }

    /**
     * Retrieves the exact schema and table name from the sql server database.
     *
     * @param connection The database connection.
     * @param tableName  TableName to validate and convert.
     * @return The exact case-sensitive TableName.
     * @throws SQLException If a database connection failures.
     */
    public static TableName getObjectNameCaseInsensitively(Connection connection, TableName tableName) throws SQLException
    {
        try (PreparedStatement stmt = connection.prepareStatement(OBJECT_NAME_QUERY_TEMPLATE)) {
            stmt.setString(1, tableName.getSchemaName().toLowerCase());
            stmt.setString(2, tableName.getTableName().toLowerCase());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    TableName matchedTable = new TableName(rs.getString("TABLE_SCHEMA"), rs.getString("TABLE_NAME"));

                    // Check if another match found
                    if (rs.next()) {
                        throw new RuntimeException(String.format("Multiple matches found for object %s.%s",
                                tableName.getSchemaName(), tableName.getTableName()));
                    }
                    // Return the exact case-sensitive schema and table name.
                    return matchedTable;
                }
            }
        }
        // Throw an exception if no matching schema and table name is found.
        throw new RuntimeException(String.format("Object %s.%s not found", tableName.getSchemaName(), tableName.getTableName()));
    }

    /**
     * Retrieves the casing mode from the provided options.
     * If the casing mode is not specified, it defaults to NONE. This applies to both Glue and non-Glue connections.
     *
     * @param configOptions Config options where the casing mode may be present.
     * @return CasingMode corresponding to the config value, or NONE if not specified.
     * @throws IllegalArgumentException If the provided casing mode value is invalid.
     */
    private static SqlserverCasingMode getCasingMode(Map<String, String> configOptions)
    {
        if (!configOptions.containsKey(CASING_MODE)) {
            LOGGER.info("CASING MODE disable");
            return SqlserverCasingMode.NONE;
        }

        try {
            SqlserverCasingMode sqlserverCasingMode = SqlserverCasingMode.valueOf(configOptions.get(CASING_MODE).toUpperCase());
            LOGGER.info("CASING MODE enable: {}", sqlserverCasingMode.toString());
            return sqlserverCasingMode;
        }
        catch (Exception ex) {
            // print error log for customer along with list of input
            LOGGER.error("Invalid input for:{}, input value:{}, valid values:{}", CASING_MODE, configOptions.get(CASING_MODE), Arrays.asList(SqlserverCasingMode.values()), ex);
            throw ex;
        }
    }
}
