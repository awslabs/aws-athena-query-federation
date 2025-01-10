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
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;

public class SnowflakeCaseInsensitiveResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeCaseInsensitiveResolver.class);
    private static final String SCHEMA_NAME_QUERY_TEMPLATE = "select * from INFORMATION_SCHEMA.SCHEMATA where lower(SCHEMA_NAME) = ?";
    private static final String TABLE_NAME_QUERY_TEMPLATE = "select * from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = ? and lower(TABLE_NAME) = ?";
    private static final String SCHEMA_NAME_COLUMN_KEY = "SCHEMA_NAME";
    private static final String TABLE_NAME_COLUMN_KEY = "TABLE_NAME";
    private static final String CASING_MODE = "casing_mode";
    private static final String ANNOTATION_CASE_UPPER = "upper";
    private static final String ANNOTATION_CASE_LOWER = "lower";

    private SnowflakeCaseInsensitiveResolver()
    {
    }

    private enum SnowflakeCasingMode
    {
        NONE,
        CASE_INSENSITIVE_SEARCH,
        ANNOTATION
    }

    public static TableName getAdjustedTableObjectNameBasedOnConfig(final Connection connection,  TableName tableName, Map<String, String> configOptions)
            throws SQLException
    {
        SnowflakeCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case CASE_INSENSITIVE_SEARCH:
                String schemaNameCaseInsensitively = getSchemaNameCaseInsensitively(connection, tableName.getSchemaName(), configOptions);
                String tableNameCaseInsensitively = getTableNameCaseInsensitively(connection, schemaNameCaseInsensitively, tableName.getTableName(), configOptions);
                TableName tableNameResult = new TableName(schemaNameCaseInsensitively, tableNameCaseInsensitively);
                LOGGER.info("casing mode is `CASE_INSENSITIVE_SEARCH`: adjusting casing from Slowflake case insensitive search for TableName object. TableName:{}", tableNameResult);
                return tableNameResult;
            case ANNOTATION:
                TableName tableNameFromQueryHint = getTableNameFromQueryHint(tableName);
                LOGGER.info("casing mode is `ANNOTATION`: adjusting casing from input if annotation found for TableName object. TableName:{}", tableNameFromQueryHint);
                return tableNameFromQueryHint;
            case NONE:
                LOGGER.info("casing mode is `NONE`: not adjust casing from input for TableName object. TableName:{}", tableName);
                return tableName;
        }
        LOGGER.warn("casing mode is empty: not adjust casing from input for TableName object. TableName:{}", tableName);
        return tableName;
    }

    public static String getAdjustedSchemaNameBasedOnConfig(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        SnowflakeCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case CASE_INSENSITIVE_SEARCH:
                LOGGER.info("casing mode is `CASE_INSENSITIVE_SEARCH`: adjusting casing from Slowflake case insensitive search for Schema...");
                return getSchemaNameCaseInsensitively(connection, schemaNameInput, configOptions);
            case NONE:
                LOGGER.info("casing mode is `NONE`: not adjust casing from input for Schema");
                return schemaNameInput;
            case ANNOTATION:
                LOGGER.info("casing mode is `ANNOTATION`: adjust casing for SCHEMA is NOT SUPPORTED. Skip casing adjustment");
        }

        return schemaNameInput;
    }

    public static String getSchemaNameCaseInsensitively(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        String nameFromSnowFlake = null;
        int i = 0;
        try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                .withConnection(connection)
                .withQuery(SCHEMA_NAME_QUERY_TEMPLATE)
                .withParameters(Arrays.asList(schemaNameInput.toLowerCase())).build();
                ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                i++;
                String schemaNameCandidate = resultSet.getString(SCHEMA_NAME_COLUMN_KEY);
                LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", SCHEMA_NAME_COLUMN_KEY, schemaNameCandidate);
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

    public static String getTableNameCaseInsensitively(final Connection connection, String schemaName, String tableNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        // schema name input should be correct case before searching tableName already
        String nameFromSnowFlake = null;
        int i = 0;
        try (PreparedStatement preparedStatement = new PreparedStatementBuilder()
                .withConnection(connection)
                .withQuery(TABLE_NAME_QUERY_TEMPLATE)
                .withParameters(Arrays.asList(schemaName, tableNameInput.toLowerCase())).build();
                ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                i++;
                String schemaNameCandidate = resultSet.getString(TABLE_NAME_COLUMN_KEY);
                LOGGER.debug("Case insensitive search on columLabel: {}, schema name: {}", TABLE_NAME_COLUMN_KEY, schemaNameCandidate);
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

    /*
    Keep previous implementation of table name casing adjustment from query hint. This is to keep backward compatibility.
     */
    public static TableName getTableNameFromQueryHint(TableName table)
    {
        LOGGER.info("getTableNameFromQueryHint: " + table);
        //if no query hints has been passed then return input table name
        if (!table.getTableName().contains("@")) {
            return new TableName(table.getSchemaName().toUpperCase(), table.getTableName().toUpperCase());
        }
        //analyze the hint to find table and schema case
        String[] tbNameWithQueryHint = table.getTableName().split("@");
        String[] hintDetails = tbNameWithQueryHint[1].split("&");
        String schemaCase = ANNOTATION_CASE_UPPER;
        String tableCase = ANNOTATION_CASE_UPPER;
        String tableName = tbNameWithQueryHint[0];
        for (String str : hintDetails) {
            String[] hintDetail = str.split("=");
            if (hintDetail[0].contains("schema")) {
                schemaCase = hintDetail[1];
            }
            else if (hintDetail[0].contains("table")) {
                tableCase = hintDetail[1];
            }
        }
        if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toLowerCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(ANNOTATION_CASE_UPPER) && tableCase.equalsIgnoreCase(ANNOTATION_CASE_LOWER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toLowerCase());
        }
        else {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
    }

    /*
    Default behavior with and without glue connection is different. As we want to make it backward compatible for customer who is not using glue connection.
    With Glue connection, default behavior is `NONE` which we will not adjust any casing in the connector.
    Without Glue connection, default behavior is `ANNOTATION` which customer can perform MY_TABLE@schemaCase=upper&tableCase=upper
     */
    private static SnowflakeCasingMode getCasingMode(Map<String, String> configOptions)
    {
        boolean isGlueConnection = StringUtils.isNotBlank(configOptions.get(DEFAULT_GLUE_CONNECTION));
        if (!configOptions.containsKey(CASING_MODE)) {
            LOGGER.info("CASING MODE disable");
            return isGlueConnection ? SnowflakeCasingMode.NONE : SnowflakeCasingMode.ANNOTATION;
        }

        try {
            SnowflakeCasingMode snowflakeCasingMode = SnowflakeCasingMode.valueOf(configOptions.get(CASING_MODE).toUpperCase());
            LOGGER.info("CASING MODE enable: {}", snowflakeCasingMode.toString());
            return snowflakeCasingMode;
        }
        catch (Exception ex) {
            // print error log for customer along with list of input
            LOGGER.error("Invalid input for:{}, input value:{}, valid values:{}", CASING_MODE, configOptions.get(CASING_MODE), Arrays.asList(SnowflakeCasingMode.values()), ex);
            throw ex;
        }
    }
}
