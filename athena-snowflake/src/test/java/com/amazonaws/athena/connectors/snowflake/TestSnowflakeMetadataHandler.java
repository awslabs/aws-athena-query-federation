/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.snowflake.resolver.SnowflakeJDBCCaseResolver;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.Map;

/**
 * Test implementation of SnowflakeMetadataHandler that provides a modified getCredentialProvider method
 * to avoid the "No secret name found for authentication" error during tests.
 */
public class TestSnowflakeMetadataHandler extends SnowflakeMetadataHandler {

    public TestSnowflakeMetadataHandler(
            DatabaseConnectionConfig databaseConnectionConfig,
            SecretsManagerClient secretsManager,
            AthenaClient athena,
            JdbcConnectionFactory jdbcConnectionFactory,
            Map<String, String> configOptions,
            SnowflakeJDBCCaseResolver jdbcCaseResolver) {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions, jdbcCaseResolver);
    }

    @Override
    protected CredentialsProvider getCredentialProvider() {
        return Mockito.mock(CredentialsProvider.class);
    }
}

