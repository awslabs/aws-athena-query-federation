/*-
 * #%L
 * athena-clickhouse
 * %%
 * Copyright (C) 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.clickhouse;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Clickhouse utilities adapted and ported from {@link JDBCUtil}.
 */
public final class ClickHouseUtil 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseUtil.class);

    private ClickHouseUtil() {}
    
    public static TableName informationSchemaCaseInsensitiveTableMatch(Connection connection, final String databaseName,
                                                     final String tableName) throws Exception
    {
        // Gets case insensitive schema name
        String resolvedSchemaName = null;
        PreparedStatement statement = getSchemaNameQuery(connection, databaseName);
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                resolvedSchemaName = resultSet.getString("schema_name");
            }
            else {
                throw new RuntimeException(String.format("During SCHEMA Case Insensitive look up could not find Database '%s'", databaseName));
            }
        }

        // passes actual cased schema name to query for tableName
        String resolvedName = null;
        statement = getTableNameQuery(connection, tableName, resolvedSchemaName);
        try (ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                resolvedName = resultSet.getString("table_name");
                if (resultSet.next()) {
                    throw new RuntimeException(String.format("More than one table that matches '%s' was returned from Database %s", tableName, databaseName));
                }
                LOGGER.debug("Resolved name from Case Insensitive look up : {}", resolvedName);
            }
            else {
                throw new RuntimeException(String.format("During TABLE Case Insensitive look up could not find Table '%s' in Database '%s'", tableName, databaseName));
            }
        }

        return new TableName(resolvedSchemaName, resolvedName);
    }

    public static PreparedStatement getTableNameQuery(Connection connection, String tableName, String databaseName) throws SQLException
    {
        String sql = "SELECT DISTINCT table_name FROM information_schema.tables WHERE (table_name = ? or lower(table_name) = ?) AND table_schema = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, tableName);
        preparedStatement.setString(2, tableName);
        preparedStatement.setString(3, databaseName);
        LOGGER.debug("Prepared Statement for getting table name with Case Insensitive Look Up in schema {} : {}", databaseName, preparedStatement);
        return preparedStatement;
    }

    public static PreparedStatement getSchemaNameQuery(Connection connection, String databaseName) throws SQLException
    {
        String sql = "SELECT schema_name FROM information_schema.schemata WHERE (schema_name = ? or lower(schema_name) = ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, databaseName);
        preparedStatement.setString(2, databaseName);
        return preparedStatement;
    }

    public static List<TableName> getTables(Connection connection, String databaseName) throws SQLException
    {
       String tablesAndViews = "Tables and Views";
       String sql = "SELECT DISTINCT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?";
       PreparedStatement preparedStatement = connection.prepareStatement(sql);
       preparedStatement.setString(1, databaseName);
       LOGGER.debug("Prepared Statement for getting tables in schema {} : {}", databaseName, preparedStatement);
       return getTableMetadata(preparedStatement, tablesAndViews);
    }

    public static List<TableName> getTableMetadata(PreparedStatement preparedStatement, String tableType)
    {
        ImmutableList.Builder<TableName> list = ImmutableList.builder();
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                list.add(getSchemaTableName(resultSet));
            }
        }
        catch (SQLException ex) {
            LOGGER.warn("Unable to return list of {} from data source!", tableType);
        }
        return list.build();
    }

    public static TableName getSchemaTableName(final ResultSet resultSet) throws SQLException
    {
        return new TableName(
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"));
    }
    
}
