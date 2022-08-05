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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.ORACLE_NAME;

/**
 * Data handler, user must have necessary permissions to read from necessary tables.
 */
public class OracleRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleRecordHandler.class);
    private static final int FETCH_SIZE = 1000;
    private static final String ORACLE_QUOTE_CHARACTER = "\"";

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link OracleMuxCompositeHandler} instead.
     */
    public OracleRecordHandler()
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(ORACLE_NAME));
    }

    public OracleRecordHandler(final DatabaseConnectionConfig databaseConnectionConfig)
    {
        this(databaseConnectionConfig, new OracleJdbcConnectionFactory(databaseConnectionConfig, new DatabaseConnectionInfo(ORACLE_DRIVER_CLASS, ORACLE_DEFAULT_PORT)));
    }

    public OracleRecordHandler(final DatabaseConnectionConfig databaseConnectionConfig, final JdbcConnectionFactory jdbcConnectionFactory)
    {
        this(databaseConnectionConfig, AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(),
                jdbcConnectionFactory, new OracleQueryStringBuilder(ORACLE_QUOTE_CHARACTER));
    }

    @VisibleForTesting
    OracleRecordHandler(final DatabaseConnectionConfig databaseConnectionConfig, final AmazonS3 amazonS3, final AWSSecretsManager secretsManager,
                        final AmazonAthena athena, final JdbcConnectionFactory jdbcConnectionFactory, final JdbcSplitQueryBuilder jdbcSplitQueryBuilder)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split)
            throws SQLException
    {
        PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);

        // Disable fetching all rows.
        preparedStatement.setFetchSize(FETCH_SIZE);

        return preparedStatement;
    }
}
