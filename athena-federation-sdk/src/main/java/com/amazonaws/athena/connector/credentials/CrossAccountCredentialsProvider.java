/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;

public class CrossAccountCredentialsProvider
{
    private static final String CROSS_ACCOUNT_ROLE_ARN_CONFIG = "cross_account_role_arn";
    private static final Logger logger = LoggerFactory.getLogger(CrossAccountCredentialsProvider.class);

    private CrossAccountCredentialsProvider() {}

    public static AWSCredentialsProvider getCrossAccountCredentialsIfPresent(Map<String, String> configOptions, String roleSessionName)
    {
        if (configOptions.containsKey(CROSS_ACCOUNT_ROLE_ARN_CONFIG)) {
            logger.debug("Found cross-account role arn to assume.");
            StsClient stsClient = StsClient.create();
            AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(configOptions.get(CROSS_ACCOUNT_ROLE_ARN_CONFIG))
                .roleSessionName(roleSessionName)
                .build();
            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);
            Credentials credentials = assumeRoleResponse.credentials();
            BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
            return new AWSStaticCredentialsProvider(basicSessionCredentials);
        }
        return DefaultAWSCredentialsProviderChain.getInstance();
    }
}
