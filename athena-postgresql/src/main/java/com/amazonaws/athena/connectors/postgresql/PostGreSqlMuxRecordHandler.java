/*-
 * #%L
 * athena-postgresql
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

package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandlerFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;

class PostGreSqlMuxRecordHandlerFactory implements JdbcRecordHandlerFactory
{
    @Override
    public String getEngine()
    {
        return POSTGRES_NAME;
    }

    @Override
    public JdbcRecordHandler createJdbcRecordHandler(DatabaseConnectionConfig config, java.util.Map<String, String> configOptions)
    {
        return new PostGreSqlRecordHandler(config, configOptions);
    }
}

public class PostGreSqlMuxRecordHandler extends MultiplexingJdbcRecordHandler
{
    public PostGreSqlMuxRecordHandler(java.util.Map<String, String> configOptions)
    {
        super(new PostGreSqlMuxRecordHandlerFactory(), configOptions);
    }

    @VisibleForTesting
    PostGreSqlMuxRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AmazonAthena athena, JdbcConnectionFactory jdbcConnectionFactory,
            DatabaseConnectionConfig databaseConnectionConfig, Map<String, JdbcRecordHandler> recordHandlerMap, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, jdbcConnectionFactory, databaseConnectionConfig, recordHandlerMap, configOptions);
    }
}