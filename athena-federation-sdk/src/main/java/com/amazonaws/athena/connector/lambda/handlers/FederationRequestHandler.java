/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;
import com.amazonaws.athena.connector.lambda.domain.spill.ExpectedBucketOwnerInterceptor;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.KmsEncryptionProvider;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.FAS_TOKEN;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SERVICE_KMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_BUCKET_OWNER;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_FAS_TOKEN;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SPILL_SERVICE_KMS;

public interface FederationRequestHandler extends RequestStreamHandler
{
    /**
     * Gets the CachableSecretsManager instance used by this handler.
     * Implementations must provide access to their secrets manager instance.
     *
     * @return The CachableSecretsManager instance
     */
    CachableSecretsManager getCachableSecretsManager();

    /**
     * Gets the KmsEncryptionProvider instance used by this handler.
     * Implementations must provide access to their KMS encryption provider instance.
     *
     * @return The KmsEncryptionProvider instance
     */
    KmsEncryptionProvider getKmsEncryptionProvider();

    /**
     * Resolves any secrets found in the supplied string, for example: MyString${WithSecret} would have ${WithSecret}
     * replaced by the corresponding value of the secret in AWS Secrets Manager with that name. If no such secret is found
     * the function throws.
     *
     * @param rawString The string in which you'd like to replace SecretsManager placeholders.
     * (e.g. ThisIsA${Secret}Here - The ${Secret} would be replaced with the contents of a SecretsManager
     * secret called Secret. If no such secret is found, the function throws. If no ${} are found in
     * the input string, nothing is replaced and the original string is returned.
     * @return The processed string with secrets resolved
     */
    default String resolveSecrets(String rawString)
    {
        return getCachableSecretsManager().resolveSecrets(rawString);
    }

    /**
     * Resolves secrets with default credentials format (username:password).
     *
     * @param rawString The string containing secret placeholders to resolve
     * @return The processed string with secrets resolved in default credentials format
     */
    default String resolveWithDefaultCredentials(String rawString)
    {
        return getCachableSecretsManager().resolveWithDefaultCredentials(rawString);
    }

    /**
     * Retrieves a secret from AWS Secrets Manager.
     *
     * @param secretName The name of the secret to retrieve
     * @return The secret value
     */
    default String getSecret(String secretName)
    {
        return getCachableSecretsManager().getSecret(secretName);
    }

    /**
     * Retrieves a secret from AWS Secrets Manager with request override configuration.
     *
     * @param secretName The name of the secret to retrieve
     * @param requestOverrideConfiguration AWS request override configuration for federated requests
     * @return The secret value
     */
    default String getSecret(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return getCachableSecretsManager().getSecret(secretName, requestOverrideConfiguration);
    }

    default AwsCredentials getSessionCredentials(String kmsKeyId,
                                                 String tokenString,
                                                 KmsEncryptionProvider kmsEncryptionProvider)
    {
        return kmsEncryptionProvider.getFasCredentials(kmsKeyId, tokenString);
    }

    /**
     * Gets the AWS request override configuration for a FederationRequest.
     * This method extracts the configuration options from the federated identity and delegates
     * to the Map-based overload.
     *
     * @param request The federation request
     * @return The AWS request override configuration, or null if not a federated request
     */
    default AwsRequestOverrideConfiguration getRequestOverrideConfig(FederationRequest request)
    {
        if (isRequestFederated(request)) {
            FederatedIdentity federatedIdentity = request.getIdentity();
            Map<String, String> connectorRequestOptions = federatedIdentity != null ? federatedIdentity.getConfigOptions() : null;

            if (connectorRequestOptions != null && connectorRequestOptions.get(FAS_TOKEN) != null) {
                return getRequestOverrideConfig(connectorRequestOptions);
            }
        }
        return null;
    }

    /**
     * Gets the AWS request override configuration for the given config options.
     * This is a convenience method that delegates to the full overload using the handler's
     * KMS encryption provider.
     *
     * @param configOptions The configuration options map
     * @return The AWS request override configuration, or null if not applicable
     */
    default AwsRequestOverrideConfiguration getRequestOverrideConfig(Map<String, String> configOptions)
    {
        return getRequestOverrideConfig(configOptions, getKmsEncryptionProvider());
    }

    default AwsRequestOverrideConfiguration getRequestOverrideConfig(Map<String, String> configOptions,
                                                                     KmsEncryptionProvider kmsEncryptionProvider)
    {
        AwsRequestOverrideConfiguration overrideConfig = null;
        if (Objects.nonNull(configOptions) && configOptions.containsKey(FAS_TOKEN)
                && configOptions.containsKey(SERVICE_KMS)) {
            AwsCredentials awsCredentials = getSessionCredentials(configOptions.get(SERVICE_KMS),
                    configOptions.get(FAS_TOKEN), kmsEncryptionProvider);
            overrideConfig = AwsRequestOverrideConfiguration.builder()
                    .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                    .build();
        }
        return overrideConfig;
    }

    default S3Client getS3Client(AwsRequestOverrideConfiguration awsRequestOverrideConfiguration, S3Client defaultS3)
    {
        if (Objects.nonNull(awsRequestOverrideConfiguration) &&
                awsRequestOverrideConfiguration.credentialsProvider().isPresent()) {
            AwsCredentialsProvider awsCredentialsProvider = awsRequestOverrideConfiguration.credentialsProvider().get();
            return S3Client.builder()
                    .credentialsProvider(awsCredentialsProvider)
                    .build();
        }
        else {
            return defaultS3;
        }
    }

    /**
     * Returns the S3 client used to write spill objects for a request.
     *
     * When the config options carry the service-managed spill channel ({@code spill_fas_token},
     * {@code spill_service_kms_key_id} and {@code spill_bucket_owner}), spill objects are written with those
     * per-query credentials and every request carries {@code x-amz-expected-bucket-owner}. The owner header is the
     * only check that the bucket written to belongs to the service: the GetSplits spill bucket check runs against
     * the Lambda's configured bucket with the caller's credentials. All three keys are required together, and any
     * partial set fails the request. With none of them this is
     * {@link #getS3Client(AwsRequestOverrideConfiguration, S3Client)} with the caller's override, so existing
     * callers are unchanged.
     *
     * When the spill channel is used the returned client is new for each request, and the caller closes it once
     * its writes are done (see {@link #isServiceManagedSpill}).
     *
     * @param configOptions the request's federated identity config options
     * @param callerOverrideConfig the caller's (tenant FAS) override, used when no spill channel is present
     * @param defaultS3 the handler's default S3 client
     */
    /**
     * True when the config options carry any service-managed spill key, i.e. when
     * {@link #getSpillS3Client} returns a per-request client the caller must close.
     */
    default boolean isServiceManagedSpill(Map<String, String> configOptions)
    {
        return configOptions != null && !StringUtils.isAllBlank(configOptions.get(SPILL_FAS_TOKEN),
                configOptions.get(SPILL_SERVICE_KMS), configOptions.get(SPILL_BUCKET_OWNER));
    }

    default S3Client getSpillS3Client(Map<String, String> configOptions,
                                      AwsRequestOverrideConfiguration callerOverrideConfig,
                                      S3Client defaultS3)
    {
        String spillToken = configOptions == null ? null : configOptions.get(SPILL_FAS_TOKEN);
        String spillKmsKeyId = configOptions == null ? null : configOptions.get(SPILL_SERVICE_KMS);
        String bucketOwner = configOptions == null ? null : configOptions.get(SPILL_BUCKET_OWNER);
        if (!isServiceManagedSpill(configOptions)) {
            return getS3Client(callerOverrideConfig, defaultS3);
        }
        if (StringUtils.isAnyBlank(spillToken, spillKmsKeyId, bucketOwner)) {
            throw new AthenaConnectorException("Service-managed spill requires " + SPILL_FAS_TOKEN + ", "
                    + SPILL_SERVICE_KMS + " and " + SPILL_BUCKET_OWNER, ErrorDetails.builder()
                    .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        ExpectedBucketOwnerInterceptor ownerInterceptor;
        try {
            ownerInterceptor = new ExpectedBucketOwnerInterceptor(bucketOwner);
        }
        catch (IllegalArgumentException e) {
            throw new AthenaConnectorException(SPILL_BUCKET_OWNER + " must be a 12-digit AWS account id",
                    ErrorDetails.builder()
                            .errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }

        AwsCredentials spillCredentials = getSessionCredentials(spillKmsKeyId, spillToken, getKmsEncryptionProvider());
        // No explicit region: the spill bucket is expected in the Lambda's region (cross-region access is off).
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(spillCredentials))
                .overrideConfiguration(c -> c.addExecutionInterceptor(ownerInterceptor))
                .build();
    }

    default AthenaClient getAthenaClient(AwsRequestOverrideConfiguration awsRequestOverrideConfiguration, AthenaClient defaultAthena)
    {
        if (Objects.nonNull(awsRequestOverrideConfiguration) &&
                awsRequestOverrideConfiguration.credentialsProvider().isPresent()) {
            AwsCredentialsProvider awsCredentialsProvider = awsRequestOverrideConfiguration.credentialsProvider().get();
            return AthenaClient.builder()
                    .credentialsProvider(awsCredentialsProvider)
                    .build();
        }
        else {
            return defaultAthena;
        }
    }

    default boolean isRequestFederated(FederationRequest req)
    {
        FederatedIdentity federatedIdentity = req.getIdentity();
        Map<String, String> connectorRequestOptions = federatedIdentity != null ? federatedIdentity.getConfigOptions() : null;
        return (connectorRequestOptions != null && connectorRequestOptions.get(FAS_TOKEN) != null);
    }
    
    default CredentialsProvider getCredentialProvider()
    {
        return getCredentialProvider(null);
    }

    /**
     * Gets a credentials provider for database connections with optional request override configuration.
     * This method checks if a secret name is configured and creates a credentials provider if available.
     * Subclasses can override createCredentialsProvider() to provide custom credential provider implementations.
     *
     * @param requestOverrideConfiguration Optional AWS request override configuration for federated requests
     * @return CredentialsProvider instance or null if no secret is configured
     */
    default CredentialsProvider getCredentialProvider(AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        final String secretName = getDatabaseConnectionSecret();
        if (StringUtils.isNotBlank(secretName)) {
            Logger logger = LoggerFactory.getLogger(this.getClass());
            logger.info("Using Secrets Manager.");
            return createCredentialsProvider(secretName, requestOverrideConfiguration);
        }
        return null;
    }

    /**
     * Factory method to create CredentialsProvider. Subclasses can override this to provide
     * custom credential provider implementations (e.g., SnowflakeCredentialsProvider).
     *
     * @param secretName The secret name to retrieve credentials from
     * @param requestOverrideConfiguration Optional AWS request override configuration
     * @return CredentialsProvider instance
     */
    default CredentialsProvider createCredentialsProvider(String secretName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
    {
        return new DefaultCredentialsProvider(getSecret(secretName, requestOverrideConfiguration));
    }

    /**
     * Gets the database connection secret name. Subclasses that use database credentials
     * should override this method to provide the secret name from their configuration.
     *
     * @return The secret name, or null if not applicable
     */
    default String getDatabaseConnectionSecret()
    {
        return null;
    }
}
