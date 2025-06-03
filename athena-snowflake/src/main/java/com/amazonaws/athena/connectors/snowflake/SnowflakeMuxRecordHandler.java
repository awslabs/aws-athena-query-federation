
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

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandlerFactory;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

class SnowflakeMuxRecordHandlerFactory implements JdbcRecordHandlerFactory
{
    @Override
    public String getEngine()
    {
        return SnowflakeConstants.SNOWFLAKE_NAME;
    }

    @Override
    public JdbcRecordHandler createJdbcRecordHandler(DatabaseConnectionConfig config, Map<String, String> configOptions)
    {
        return new SnowflakeRecordHandler(config, configOptions);
    }
}

public class SnowflakeMuxRecordHandler extends MultiplexingJdbcRecordHandler
{
    public SnowflakeMuxRecordHandler(Map<String, String> configOptions)
    {
        super(new SnowflakeMuxRecordHandlerFactory(), configOptions);
    }

    @VisibleForTesting
    SnowflakeMuxRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory,
                                     DatabaseConnectionConfig databaseConnectionConfig, Map<String, JdbcRecordHandler> recordHandlerMap, Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, jdbcConnectionFactory, databaseConnectionConfig, recordHandlerMap, configOptions);
    }
}
