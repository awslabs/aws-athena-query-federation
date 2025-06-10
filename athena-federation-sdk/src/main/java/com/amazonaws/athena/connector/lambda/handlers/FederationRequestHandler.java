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

import com.amazonaws.athena.connector.lambda.security.KmsEncryptionProvider;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;
import java.util.Objects;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.FAS_TOKEN;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SERVICE_KMS;

public interface FederationRequestHandler extends RequestStreamHandler
{
    default AwsCredentials getSessionCredentials(String kmsKeyId,
                                                 String tokenString,
                                                 KmsEncryptionProvider kmsEncryptionProvider)
    {
        return kmsEncryptionProvider.getFasCredentials(kmsKeyId, tokenString);
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
}
