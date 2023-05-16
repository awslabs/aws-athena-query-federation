/*-
 * #%L
 * athena-cloudera-impala
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.hortonworks.ImpalaFederationExpressionParser;
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.FETCH_SIZE;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_NAME;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_QUOTE_CHARACTER;
import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.JDBC_PROPERTIES;

public class ImpalaRecordHandler extends JdbcRecordHandler
{
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    public ImpalaRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(IMPALA_NAME, configOptions), configOptions);
    }
    public ImpalaRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new ImpalaJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(IMPALA_DRIVER_CLASS, IMPALA_DEFAULT_PORT)), configOptions);
    }
    public ImpalaRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(),
                jdbcConnectionFactory, new ImpalaQueryStringBuilder(IMPALA_QUOTE_CHARACTER, new ImpalaFederationExpressionParser(IMPALA_QUOTE_CHARACTER)), configOptions);
    }
    @VisibleForTesting
    ImpalaRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }
    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split) throws SQLException
    {
        PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
        preparedStatement.setFetchSize(FETCH_SIZE);
        return preparedStatement;
    }
}
