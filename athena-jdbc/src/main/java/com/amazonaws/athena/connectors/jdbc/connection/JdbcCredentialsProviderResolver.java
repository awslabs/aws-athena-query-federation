/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.handlers.FederationRequestHandler;
import com.amazonaws.athena.connectors.jdbc.credentials.RdsIamAuthConfiguration;
import com.amazonaws.athena.connectors.jdbc.credentials.RdsIamCredentialsProvider;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves JDBC credentials for Secrets Manager, inline, or RDS IAM authentication.
 * IAM providers are cached by {@link RdsIamAuthConfiguration} so token cache can be reused
 * across connections.
 */
public final class JdbcCredentialsProviderResolver
{
    private static final ConcurrentHashMap<RdsIamAuthConfiguration, CredentialsProvider> IAM_PROVIDER_CACHE =
            new ConcurrentHashMap<>();

    private JdbcCredentialsProviderResolver() {}

    public static CredentialsProvider resolve(
            final DatabaseConnectionConfig databaseConnectionConfig,
            final FederationRequestHandler handler,
            final AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        if (databaseConnectionConfig != null && databaseConnectionConfig.isIamAuthEnabled()) {
            return IAM_PROVIDER_CACHE.computeIfAbsent(
                    databaseConnectionConfig.getIamAuthConfiguration(),
                    RdsIamCredentialsProvider::new);
        }

        final String secretName = databaseConnectionConfig != null ? databaseConnectionConfig.getSecret() : handler.getDatabaseConnectionSecret();
        if (StringUtils.isNotBlank(secretName)) {
            return handler.createCredentialsProvider(secretName, requestOverrideConfiguration);
        }
        return null;
    }

    @VisibleForTesting
    static void clearIamProviderCache()
    {
        IAM_PROVIDER_CACHE.clear();
    }
}
