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
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.mysql.MySqlMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.mysql.MySqlRecordHandler;
import com.amazonaws.athena.connectors.jdbc.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.postgresql.PostGreSqlRecordHandler;
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
     * @param databaseEngine database type. See {@link JdbcConnectionFactory.DatabaseEngine}.
     * @return database connection confuiguration. See {@link DatabaseConnectionConfig}.
     */
    public static DatabaseConnectionConfig getSingleDatabaseConfigFromEnv(final JdbcConnectionFactory.DatabaseEngine databaseEngine)
    {
        List<DatabaseConnectionConfig> databaseConnectionConfigs = DatabaseConnectionConfigBuilder.buildFromSystemEnv();

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            if (DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY.equals(databaseConnectionConfig.getCatalog())
                    && databaseEngine.equals(databaseConnectionConfig.getType())) {
                return databaseConnectionConfig;
            }
        }

        throw new RuntimeException(String.format("Must provide default connection string parameter %s for database type %s",
                DatabaseConnectionConfigBuilder.DEFAULT_CONNECTION_STRING_PROPERTY, databaseEngine.getDbName()));
    }

    /**
     * Creates a map of Catalog to respective metadata handler to be used by Multiplexer.
     *
     * @param properties system properties.
     * @return Map of String -> {@link JdbcMetadataHandler}
     */
    public static Map<String, JdbcMetadataHandler> createJdbcMetadataHandlerMap(final Map<String, String> properties)
    {
        ImmutableMap.Builder<String, JdbcMetadataHandler> metadataHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(properties.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().properties(properties).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new RuntimeException("At least one connection string required.");
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcMetadataHandler jdbcMetadataHandler = createJdbcMetadataHandler(databaseConnectionConfig);
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

    private static JdbcMetadataHandler createJdbcMetadataHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        switch (databaseConnectionConfig.getType()) {
            case MYSQL:
                return new MySqlMetadataHandler(databaseConnectionConfig);
            case REDSHIFT:
            case POSTGRES:
                return new PostGreSqlMetadataHandler(databaseConnectionConfig);
            default:
                throw new RuntimeException("Mux: Unhandled database engine " + databaseConnectionConfig.getType());
        }
    }

    /**
     * Creates a map of Catalog to respective record handler to be used by Multiplexer.
     *
     * @param properties system properties.
     * @return Map of String -> {@link JdbcRecordHandler}
     */
    public static Map<String, JdbcRecordHandler> createJdbcRecordHandlerMap(final Map<String, String> properties)
    {
        ImmutableMap.Builder<String, JdbcRecordHandler> recordHandlerMap = ImmutableMap.builder();

        final String functionName = Validate.notBlank(properties.get(LAMBDA_FUNCTION_NAME_PROPERTY), "Lambda function name not present in environment.");
        List<DatabaseConnectionConfig> databaseConnectionConfigs = new DatabaseConnectionConfigBuilder().properties(properties).build();

        if (databaseConnectionConfigs.isEmpty()) {
            throw new RuntimeException("At least one connection string required.");
        }

        boolean defaultPresent = false;

        for (DatabaseConnectionConfig databaseConnectionConfig : databaseConnectionConfigs) {
            JdbcRecordHandler jdbcRecordHandler = createJdbcRecordHandler(databaseConnectionConfig);
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

    private static JdbcRecordHandler createJdbcRecordHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        switch (databaseConnectionConfig.getType()) {
            case MYSQL:
                return new MySqlRecordHandler(databaseConnectionConfig);
            case REDSHIFT:
            case POSTGRES:
                return new PostGreSqlRecordHandler(databaseConnectionConfig);
            default:
                throw new RuntimeException("Mux: Unhandled database engine " + databaseConnectionConfig.getType());
        }
    }
}
