/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.snowflake.connection.SnowflakeConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.credentials.SnowflakePrivateKeyCredentialProvider;
import com.amazonaws.athena.connectors.snowflake.utils.SnowflakeAuthUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeMetadataHandler.JDBC_PROPERTIES;

public class SnowflakeRecordHandler extends JdbcRecordHandler
{
    private static final int FETCH_SIZE = 1000;
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeRecordHandler.class);
    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link SnowflakeMuxCompositeHandler} instead.
     */
    public SnowflakeRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
    }
    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new SnowflakeConnectionFactory(databaseConnectionConfig,
                SnowflakeEnvironmentProperties.getSnowFlakeParameter(JDBC_PROPERTIES, configOptions),
                new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS,
                        SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)), configOptions);
    }

    public SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, GenericJdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                jdbcConnectionFactory, new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER)), configOptions);
        
        // Extract parameters from connection string (for logging only)
        String connectionString = System.getenv("JDBC_CONNECTION_STRING");
        if (connectionString == null) {
            connectionString = System.getenv("default");
        }
        
        if (connectionString != null) {
            LOGGER.info("Processing connection string for parameters in RecordHandler");
            // Only extract parameters, without updating configOptions
            Map<String, String> extractedParams = SnowflakeAuthUtils.extractParametersFromConnectionString(connectionString);
            LOGGER.info("Extracted {} parameters from connection string", extractedParams.size());
        }
    }

    @VisibleForTesting
    SnowflakeRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, final S3Client amazonS3, final SecretsManagerClient secretsManager,
                           final AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableNameInput, Schema schema, Constraints constraints, Split split) throws SQLException
    {
        PreparedStatement preparedStatement;
        try {
            if (constraints.isQueryPassThrough()) {
                preparedStatement = buildQueryPassthroughSql(jdbcConnection, constraints);
            }
            else {
                preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableNameInput.getSchemaName(), tableNameInput.getTableName(), schema, constraints, split);
            }

            // Disable fetching all rows.
            preparedStatement.setFetchSize(FETCH_SIZE);
        }
        catch (SQLException e) {
            throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString()).build());
        }
        return preparedStatement;
    }

    @Override
    protected CredentialsProvider getCredentialProvider()
    {
        LOGGER.debug("getCredentialProvider called");

        // Get connection string
        String connectionString = System.getenv("JDBC_CONNECTION_STRING");
        if (connectionString == null) {
            connectionString = System.getenv("default");
        }

        // Get authentication type (from configOptions or connection string)
        String authType = configOptions.getOrDefault(SnowflakeConstants.AUTH_TYPE,
                SnowflakeConstants.AUTH_TYPE_PASSWORD);

        // Extract authentication type from connection string (if exists)
        if (connectionString != null) {
            String extractedAuthType = SnowflakeAuthUtils.extractParameterFromConnectionString(connectionString, "auth_type");
            if (extractedAuthType != null) {
                authType = extractedAuthType;
                LOGGER.debug("Using auth_type from connection string: {}", authType);
            }
        }
        // Get secret name (from connection string or environment variables)
        String secretName = null;

        // 1. Extract secret name from connection string
        if (connectionString != null) {
            // Extract secret_name parameter
            secretName = SnowflakeAuthUtils.extractParameterFromConnectionString(connectionString, "secret_name");

            // Extract ${secretName} format (if secret_name parameter doesn't exist)
            if (secretName == null && connectionString.contains("${") && connectionString.contains("}")) {
                secretName = connectionString.replaceAll(".*\\$\\{([^}]*)\\}.*", "$1");
                LOGGER.debug("Extracted secret_name from placeholder: {}", secretName);
            }
        }

        // 2. Get secret name from environment variable
        if (secretName == null) {
            secretName = System.getenv("secret_name");
            if (secretName != null) {
                LOGGER.debug("Using secret_name from environment variable: {}", secretName);
            }
        }

        // 3. Get secret name from configOptions
        if (secretName == null) {
            secretName = configOptions.get("secret_name");
            if (secretName != null) {
                LOGGER.debug("Using secret_name from configOptions: {}", secretName);
            }
        }

        // Error if no secret name is found
        if (secretName == null || secretName.isEmpty()) {
            LOGGER.error("No secret name found for authentication");
            throw new RuntimeException("No secret name found for authentication");
        }

        // Create SecretsManager client
        SecretsManagerClient secretsManager = SnowflakeAuthUtils.getSecretsManager();

        // Create credentials provider based on authentication type
        if (SnowflakeConstants.AUTH_TYPE_PRIVATE_KEY.equals(authType)) {
            LOGGER.debug("Creating SnowflakePrivateKeyCredentialProvider with secret: {}", secretName);
            try {
                return new SnowflakePrivateKeyCredentialProvider(secretsManager, secretName);
            }
            catch (Exception e) {
                LOGGER.error("Failed to create SnowflakePrivateKeyCredentialProvider", e);
                throw new RuntimeException("Failed to create private key credentials provider: " + e.getMessage(), e);
            }
        }
        else {
            // For password authentication
            LOGGER.debug("Creating DefaultCredentialsProvider with secret: {}", secretName);
            try {
                String secretValue = SnowflakeAuthUtils.getSecret(secretsManager, secretName);
                return new DefaultCredentialsProvider(secretValue);
            }
            catch (Exception e) {
                LOGGER.error("Failed to create DefaultCredentialsProvider", e);
                throw new RuntimeException("Failed to create password credentials provider: " + e.getMessage(), e);
            }
        }
    }
}
