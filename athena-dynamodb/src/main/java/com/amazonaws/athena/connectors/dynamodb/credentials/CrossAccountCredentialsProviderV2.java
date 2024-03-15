/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb.credentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;

public class CrossAccountCredentialsProviderV2
{
    private static final String CROSS_ACCOUNT_ROLE_ARN_CONFIG = "cross_account_role_arn";
    private static final Logger logger = LoggerFactory.getLogger(CrossAccountCredentialsProviderV2.class);

    private CrossAccountCredentialsProviderV2() {}

    public static AwsCredentialsProvider getCrossAccountCredentialsIfPresent(Map<String, String> configOptions, String roleSessionName)
    {
        if (configOptions.containsKey(CROSS_ACCOUNT_ROLE_ARN_CONFIG)) {
            logger.debug("Found cross-account role arn to assume.");
            StsClient stsClient = StsClient.builder().build();
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                    .roleArn(configOptions.get(CROSS_ACCOUNT_ROLE_ARN_CONFIG))
                    .roleSessionName(roleSessionName)
                    .build();
            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);
            Credentials credentials = assumeRoleResponse.credentials();
            AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
            return StaticCredentialsProvider.create(sessionCredentials);
        }
        return DefaultCredentialsProvider.create();
    }
}
