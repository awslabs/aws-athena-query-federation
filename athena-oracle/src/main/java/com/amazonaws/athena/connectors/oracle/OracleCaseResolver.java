
/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;

public class OracleCaseResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleCaseResolver.class);

    // the environment variable that can be set to specify which casing mode to use
    static final String CASING_MODE = "casing_mode";

    private static final String ORACLE_QUOTE_CHARACTER = "\"";

    private OracleCaseResolver() {}

    private enum OracleCasingMode
    {
        LOWER,      // casing mode to use whatever the engine returns (in trino's case, lower)
        UPPER,      // casing mode to upper case everything (oracle by default upper cases everything)
        SEARCH      // casing mode to perform case insensitive search
    }

    public static TableName getAdjustedTableObjectName(final Connection connection, TableName tableName, Map<String, String> configOptions)
            throws SQLException
    {
        OracleCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case SEARCH:
                String schemaNameCaseInsensitively = getSchemaNameCaseInsensitively(connection, tableName.getSchemaName(), configOptions);
                String tableNameCaseInsensitively = getTableNameCaseInsensitively(connection, schemaNameCaseInsensitively, tableName.getTableName(), configOptions);
                TableName tableNameResult = new TableName(schemaNameCaseInsensitively, tableNameCaseInsensitively);
                LOGGER.info("casing mode is `SEARCH`: performing case insensitive search for TableName object. TableName:{}", tableNameResult);
                return tableNameResult;
            case UPPER:
                TableName upperTableName = new TableName(tableName.getSchemaName().toUpperCase(), tableName.getTableName().toUpperCase());
                LOGGER.info("casing mode is `UPPER`: adjusting casing from input to upper case for TableName object. TableName:{}", upperTableName);
                return upperTableName;
            case LOWER:
                LOGGER.info("casing mode is `LOWER`: not adjust casing from input for TableName object. TableName:{}", tableName);
                return tableName;
        }
        LOGGER.warn("casing mode is empty: not adjust casing from input for TableName object. TableName:{}", tableName);
        return tableName;
    }

    public static String getAdjustedSchemaName(final Connection connection, String schemaNameInput, Map<String, String> configOptions)
            throws SQLException
    {
        OracleCasingMode casingMode = getCasingMode(configOptions);
        switch (casingMode) {
            case SEARCH:
                LOGGER.info("casing mode is SEARCH: performing case insensitive search for Schema...");
                return getSchemaNameCaseInsensitively(connection, schemaNameInput, configOptions);
            case UPPER:
                LOGGER.info("casing mode is `UPPER`: adjusting casing from input to upper case for Schema");
                return schemaNameInput.toUpperCase();
            case LOWER:
                LOGGER.info("casing mode is `LOWER`: not adjust casing from input for Schema");
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

    private static OracleCasingMode getCasingMode(Map<String, String> configOptions)
    {
        boolean isGlueConnection = StringUtils.isNotBlank(configOptions.get(DEFAULT_GLUE_CONNECTION));
        if (!configOptions.containsKey(CASING_MODE)) {
            LOGGER.info("CASING MODE not set");
            return isGlueConnection ? OracleCasingMode.LOWER : OracleCasingMode.UPPER;
        }

        try {
            OracleCasingMode oracleCasingMode = OracleCasingMode.valueOf(configOptions.get(CASING_MODE).toUpperCase());
            LOGGER.info("CASING MODE enable: {}", oracleCasingMode.toString());
            return oracleCasingMode;
        }
        catch (Exception ex) {
            // print error log for customer along with list of input
            LOGGER.error("Invalid input for:{}, input value:{}, valid values:{}", CASING_MODE, configOptions.get(CASING_MODE), Arrays.asList(OracleCasingMode.values()), ex);
            throw ex;
        }
    }
}
