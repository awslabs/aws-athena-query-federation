/*-
 * #%L
 * athena-cloudera-impala
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

package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connectors.jdbc.MultiplexingJdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandlerFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

import static com.amazonaws.athena.connectors.cloudera.ImpalaConstants.IMPALA_NAME;

class ImpalaMuxRecordHandlerFactory implements JdbcRecordHandlerFactory
{
    @Override
    public String getEngine()
    {
        return IMPALA_NAME;
    }

    @Override
    public JdbcRecordHandler createJdbcRecordHandler(DatabaseConnectionConfig config, java.util.Map<String, String> configOptions)
    {
        return new ImpalaRecordHandler(config, configOptions);
    }
}

public class ImpalaMuxRecordHandler extends MultiplexingJdbcRecordHandler
{
    public ImpalaMuxRecordHandler(java.util.Map<String, String> configOptions)
    {
        super(new ImpalaMuxRecordHandlerFactory(), configOptions);
    }

    @VisibleForTesting
    ImpalaMuxRecordHandler(AmazonS3 amazonS3, SecretsManagerClient secretsManager, AmazonAthena athena, JdbcConnectionFactory jdbcConnectionFactory,
                           DatabaseConnectionConfig databaseConnectionConfig, Map<String, JdbcRecordHandler> recordHandlerMap, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, jdbcConnectionFactory, databaseConnectionConfig, recordHandlerMap, configOptions);
    }
}