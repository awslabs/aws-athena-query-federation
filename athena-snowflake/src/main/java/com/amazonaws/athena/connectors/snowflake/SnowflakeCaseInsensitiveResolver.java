/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class SnowflakeCaseInsensitiveResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeCaseInsensitiveResolver.class);
    private static final String SCHEMA_NAME_QUERY = "select * from INFORMATION_SCHEMA.SCHEMATA where lower(SCHEMA_NAME) = ";
    private static final String TABLE_NAME_QUERY = "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ";
    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMA_NAME";
    private static final String TABLE_NAME_COLUMN_KEY = "TABLE_NAME";

    private static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";

    private SnowflakeCaseInsensitiveResolver()
    {
    }

    public static TableName getTableNameObjectCaseInsensitiveMatch(final Connection connection,  TableName tableName, Map<String, String> configOptions)
            throws SQLException
    {
        if (!isCaseInsensitiveMatchEnable(configOptions)) {
            return tableName;
        }

        String schemaNameCaseInsensitively = getSchemaNameCaseInsensitively(connection, tableName.getSchemaName(), configOptions);
        String tableNameCaseInsensitively = getTableNameCaseInsensitively(connection, schemaNameCaseInsensitively, tableName.getTableName(), configOptions);

        return new TableName(schemaNameCaseInsensitively, tableNameCaseInsensitively);
    }

    public static String getSchemaNameCaseInsensitively(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        if (!isCaseInsensitiveMatchEnable(configOptions)) {
            return schemaNameInput;
        }

        return getNameCaseInsensitively(connection, SCHEMA_NAME_COLUMN_KEY, SCHEMA_NAME_QUERY + "'" + schemaNameInput.toLowerCase() + "'", configOptions);
    }

    public static String getTableNameCaseInsensitively(final Connection connection, String schemaName, String tableNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        if (!isCaseInsensitiveMatchEnable(configOptions)) {
            return tableNameInput;
        }
        //'?' and lower(TABLE_NAME) = '?'
        return getNameCaseInsensitively(connection, TABLE_NAME_COLUMN_KEY, TABLE_NAME_QUERY + "'" + schemaName + "' and lower(TABLE_NAME) = '" + tableNameInput.toLowerCase() + "'", configOptions);
    }

    public static String getNameCaseInsensitively(final Connection connection, String columnLabel, String query, Map<String, String> configOptions)
            throws SQLException
    {
        LOGGER.debug("getNameCaseInsensitively, query:" + query);
        String nameFromSnowFlake = null;
        int i = 0;
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                i++;
                String schemaNameCandidate = resultSet.getString(columnLabel);
                LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", columnLabel, schemaNameCandidate);
                nameFromSnowFlake = schemaNameCandidate;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (i == 0 || i > 1) {
            throw new RuntimeException(String.format("Schema name case insensitive match failed, number of match : %d", i));
        }

        return nameFromSnowFlake;
    }

    private static boolean isCaseInsensitiveMatchEnable(Map<String, String> configOptions)
    {
        String enableCaseInsensitiveMatchEnvValue = configOptions.getOrDefault(ENABLE_CASE_INSENSITIVE_MATCH, "false").toLowerCase();
        boolean enableCaseInsensitiveMatch = enableCaseInsensitiveMatchEnvValue.equals("true");
        LOGGER.info("{} environment variable set to: {}. Resolved to: {}",
                ENABLE_CASE_INSENSITIVE_MATCH, enableCaseInsensitiveMatchEnvValue, enableCaseInsensitiveMatch);

        return enableCaseInsensitiveMatch;
    }
}
