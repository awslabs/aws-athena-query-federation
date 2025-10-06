/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.manager;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfigBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public final class JDBCUtil
{
    private static final String DEFAULT_CATALOG_PREFIX = "lambda:";
    private static final String LAMBDA_FUNCTION_NAME_PROPERTY = "AWS_LAMBDA_FUNCTION_NAME";
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCUtil.class);

    private JDBCUtil() {}

    /**
     * Extracts default database configuration for a database. Used when a specific database instance handler is used by Lambda function.
     *
     * @param databaseEngine database type.
     * @return database connection confuiguration. See {@link DatabaseConnectionConfig}.
     */
    public static DatabaseConnectionConfig getSingleDatabaseConfigFromEnv(String databaseEngine, java.util.Map<String, String> configOptions)
    {
        List<DatabaseConnectionConfig> databaseConnectionConfigs = DatabaseConnectionConfigBuilder.buildFromSystemEnv(databaseEngine, configOptions);

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())
                    && databaseEngine.equals(databaseConnectionConfig.getEngine())) {
                return databaseConnectionConfig;
            }
        }

        throw new AthenaConnectorException(String.format("Must provide default connection string parameter %s for database type %s",
                DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY, databaseEngine),
                ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
    }

    /**
     * Creates a map of Catalog to respective metadata handler to be used by Multiplexer.
     *
     * @param configOptions system configOptions.
     * @param metadataHandlerFactory factory for creating the appropriate metadata handler for the database type
     * @return Map of String -> {@link JdbcMetadataHandler}
     */
    public static Map<String, JdbcMetadataHandler> createJdbcMetadataHandlerMap(
            final Map<String, String> configOptions, JdbcMetadataHandlerFactory metadataHandlerFactory)
    {
        ImmutableMap.Builder<String, JdbcMetadataHandler> metadataHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(configOptions.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().engine(metadataHandlerFactory.getEngine()).properties(configOptions).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new AthenaConnectorException("At least one connection string required.",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcMetadataHandler jdbcMetadataHandler = metadataHandlerFactory.createJdbcMetadataHandler(databaseConnectionConfig, configOptions);
            metadataHandlerMap.put(databaseConnectionConfig.getCatalog(), jdbcMetadataHandler);

            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())) {
                metadataHandlerMap.put(DEFAULT_CATALOG_PREFIX + functionName, jdbcMetadataHandler);
                defaultPresent = true;
            }
        }

        if (!defaultPresent) {
            throw new AthenaConnectorException("Must provide connection parameters for default database instance " +
                    DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        return metadataHandlerMap.build();
    }

    /**
     * Creates a map of Catalog to respective record handler to be used by Multiplexer.
     *
     * @param configOptions system configOptions.
     * @param jdbcRecordHandlerFactory
     * @return Map of String -> {@link JdbcRecordHandler}
     */
    public static Map<String, JdbcRecordHandler> createJdbcRecordHandlerMap(Map<String, String> configOptions, JdbcRecordHandlerFactory jdbcRecordHandlerFactory)
    {
        ImmutableMap.Builder<String, JdbcRecordHandler> recordHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(configOptions.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().engine(jdbcRecordHandlerFactory.getEngine()).properties(configOptions).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new AthenaConnectorException("At least one connection string required.",
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcRecordHandler jdbcRecordHandler = jdbcRecordHandlerFactory.createJdbcRecordHandler(databaseConnectionConfig, configOptions);
            recordHandlerMap.put(databaseConnectionConfig.getCatalog(), jdbcRecordHandler);

            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())) {
                recordHandlerMap.put(DEFAULT_CATALOG_PREFIX + functionName, jdbcRecordHandler);
                defaultPresent = true;
            }
        }

        if (!defaultPresent) {
            throw new AthenaConnectorException("Must provide connection parameters for default database instance " +
                    DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY,
                    ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        return recordHandlerMap.build();
    }

    public static List<TableName> getTables(Connection connection, String databaseName) throws SQLException
    {
       String tablesAndViews = "Tables and Views";
       String sql = "SELECT table_name as \"TABLE_NAME\", table_schema as \"TABLE_SCHEM\" FROM information_schema.tables WHERE table_schema = ?";
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
            LOGGER.warn("Unable to return list of {} from data source!. Returning Empty list of table", tableType, ex);
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
