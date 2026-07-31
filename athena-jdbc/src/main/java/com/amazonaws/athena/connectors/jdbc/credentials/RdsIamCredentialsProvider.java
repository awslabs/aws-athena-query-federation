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
package com.amazonaws.athena.connectors.jdbc.credentials;

import com.amazonaws.athena.connector.credentials.Credentials;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.time.Instant;
import java.util.Map;

/**
 * Provides RDS IAM database authentication credentials by generating a short-lived auth token
 * and supplying it as the JDBC password.
 */
public class RdsIamCredentialsProvider
        implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RdsIamCredentialsProvider.class);

    /**
     * IAM auth tokens are valid for 15 minutes. Refresh before expiry when re-used across connections.
     */
    static final long TOKEN_TTL_MS = 14 * 60 * 1000L + 30 * 1000L;

    private final RdsIamAuthConfiguration iamAuthConfiguration;
    private final RdsIamAuthTokenGenerator tokenGenerator;
    private final AwsCredentialsProvider awsCredentialsProvider;

    private volatile CachedToken cachedToken;

    public RdsIamCredentialsProvider(final RdsIamAuthConfiguration iamAuthConfiguration)
    {
        this(iamAuthConfiguration, new RdsIamAuthTokenGenerator(), DefaultCredentialsProvider.builder().build());
    }

    @VisibleForTesting
    RdsIamCredentialsProvider(
            final RdsIamAuthConfiguration iamAuthConfiguration,
            final RdsIamAuthTokenGenerator tokenGenerator,
            final AwsCredentialsProvider awsCredentialsProvider)
    {
        this.iamAuthConfiguration = Validate.notNull(iamAuthConfiguration, "iamAuthConfiguration must not be null");
        this.tokenGenerator = Validate.notNull(tokenGenerator, "tokenGenerator must not be null");
        this.awsCredentialsProvider = Validate.notNull(awsCredentialsProvider, "awsCredentialsProvider must not be null");
    }

    @Override
    public Credentials getCredential()
    {
        return new DefaultCredentials(iamAuthConfiguration.getUsername(), getOrGenerateToken());
    }

    @Override
    public Map<String, String> getCredentialMap()
    {
        return getCredential().getProperties();
    }

    private String getOrGenerateToken()
    {
        final Instant now = Instant.now();
        final CachedToken current = cachedToken;
        if (current != null && now.isBefore(current.expiresAt)) {
            return current.token;
        }

        synchronized (this) {
            final CachedToken refreshed = cachedToken;
            if (refreshed != null && now.isBefore(refreshed.expiresAt)) {
                return refreshed.token;
            }

            LOGGER.info("Generating RDS IAM database authentication token for user {}", iamAuthConfiguration.getUsername());
            try {
                final String token = tokenGenerator.generateAuthToken(
                        iamAuthConfiguration.getHostname(),
                        iamAuthConfiguration.getPort(),
                        iamAuthConfiguration.getUsername(),
                        iamAuthConfiguration.getRegion(),
                        awsCredentialsProvider);
                cachedToken = new CachedToken(token, now.plusMillis(TOKEN_TTL_MS));
                return token;
            }
            catch (RuntimeException ex) {
                throw new AthenaConnectorException("Failed to generate RDS IAM authentication token: " + ex.getMessage(),
                        ErrorDetails.builder()
                                .errorCode(FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION.toString())
                                .errorMessage(ex.getMessage())
                                .build());
            }
        }
    }

    private static final class CachedToken
    {
        private final String token;
        private final Instant expiresAt;

        private CachedToken(final String token, final Instant expiresAt)
        {
            this.token = token;
            this.expiresAt = expiresAt;
        }
    }
}
