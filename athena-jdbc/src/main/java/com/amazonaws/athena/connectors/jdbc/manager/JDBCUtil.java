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

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfigBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;

public final class JDBCUtil
{
    private static final String DEFAULT_CATALOG_PREFIX = "lambda:";
    private static final String LAMBDA_FUNCTION_NAME_PROPERTY = "AWS_LAMBDA_FUNCTION_NAME";

    private JDBCUtil() {}

    /**
     * Extracts default database configuration for a database. Used when a specific database instance handler is used by Lambda function.
     *
     * @param databaseEngine database type.
     * @return database connection confuiguration. See {@link DatabaseConnectionConfig}.
     */
    public static DatabaseConnectionConfig getSingleDatabaseConfigFromEnv(final String databaseEngine)
    {
        List<DatabaseConnectionConfig> databaseConnectionConfigs = DatabaseConnectionConfigBuilder.buildFromSystemEnv(databaseEngine);

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())
                    && databaseEngine.equals(databaseConnectionConfig.getEngine())) {
                return databaseConnectionConfig;
            }
        }

        throw new RuntimeException(String.format("Must provide default connection string parameter %s for database type %s",
                DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY, databaseEngine));
    }

    /**
     * Creates a map of Catalog to respective metadata handler to be used by Multiplexer.
     *
     * @param properties system properties.
     * @param metadataHandlerFactory factory for creating the appropriate metadata handler for the database type
     * @return Map of String -> {@link JdbcMetadataHandler}
     */
    public static Map<String, JdbcMetadataHandler> createJdbcMetadataHandlerMap(
            final Map<String, String> properties, JdbcMetadataHandlerFactory metadataHandlerFactory)
    {
        ImmutableMap.Builder<String, JdbcMetadataHandler> metadataHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(properties.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().engine(metadataHandlerFactory.getEngine()).properties(properties).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new RuntimeException("At least one connection string required.");
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcMetadataHandler jdbcMetadataHandler = metadataHandlerFactory.createJdbcMetadataHandler(databaseConnectionConfig);
            metadataHandlerMap.put(databaseConnectionConfig.getCatalog(), jdbcMetadataHandler);

            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())) {
                metadataHandlerMap.put(DEFAULT_CATALOG_PREFIX + functionName, jdbcMetadataHandler);
                defaultPresent = true;
            }
        }

        if (!defaultPresent) {
            throw new RuntimeException("Must provide connection parameters for default database instance " + DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY);
        }

        return metadataHandlerMap.build();
    }

    /**
     * Creates a map of Catalog to respective record handler to be used by Multiplexer.
     *
     * @param properties system properties.
     * @param jdbcRecordHandlerFactory
     * @return Map of String -> {@link JdbcRecordHandler}
     */
    public static Map<String, JdbcRecordHandler> createJdbcRecordHandlerMap(final Map<String, String> properties, JdbcRecordHandlerFactory jdbcRecordHandlerFactory)
    {
        ImmutableMap.Builder<String, JdbcRecordHandler> recordHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(properties.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().engine(jdbcRecordHandlerFactory.getEngine()).properties(properties).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new RuntimeException("At least one connection string required.");
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcRecordHandler jdbcRecordHandler = jdbcRecordHandlerFactory.createJdbcRecordHandler(databaseConnectionConfig);
            recordHandlerMap.put(databaseConnectionConfig.getCatalog(), jdbcRecordHandler);

            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())) {
                recordHandlerMap.put(DEFAULT_CATALOG_PREFIX + functionName, jdbcRecordHandler);
                defaultPresent = true;
            }
        }

        if (!defaultPresent) {
            throw new RuntimeException("Must provide connection parameters for default database instance " + DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY);
        }

        return recordHandlerMap.build();
    }
}
