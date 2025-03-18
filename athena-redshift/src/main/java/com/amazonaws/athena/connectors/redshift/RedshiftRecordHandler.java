/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMetadataHandler;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlMuxCompositeHandler;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlQueryStringBuilder;
import com.amazonaws.athena.connectors.postgresql.PostGreSqlRecordHandler;
import com.amazonaws.athena.connectors.postgresql.PostgreSqlFederationExpressionParser;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.SQLException;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.redshift.RedshiftConstants.REDSHIFT_NAME;

public class RedshiftRecordHandler
        extends PostGreSqlRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftRecordHandler.class);

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
     */
    public RedshiftRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(REDSHIFT_NAME, configOptions), configOptions);
    }

    public RedshiftRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, PostGreSqlMetadataHandler.JDBC_PROPERTIES,
                        new DatabaseConnectionInfo(REDSHIFT_DRIVER_CLASS, REDSHIFT_DEFAULT_PORT)),
                        new PostGreSqlQueryStringBuilder(POSTGRES_QUOTE_CHARACTER, new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER)), configOptions);
    }

    @VisibleForTesting
    RedshiftRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, configOptions);
    }

    @Override
    protected boolean enableCaseSensitivelyLookUpSession(Connection connection) {
        try {
            return connection.createStatement().execute("SET enable_case_sensitive_identifier to TRUE;");
        } catch (SQLException e) {
            LOGGER.error("Failed to set enable_case_sensitive_identifier to TRUE for Redshift, ignore setting....", e);
        }
        return false;
    }

    @Override
    protected boolean disableCaseSensitivelyLookUpSession(Connection connection) {
        try {
            return connection.createStatement().execute("SET enable_case_sensitive_identifier to FALSE;");
        } catch (SQLException e) {
            LOGGER.error("Failed to set enable_case_sensitive_identifier to FALSE for Redshift, ignore setting....", e);
        }
        return false;
    }
}
