/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.credentials;

import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

/**
 * Generates RDS IAM database authentication tokens using AWS SDK for Java 2.x.
 */
public class RdsIamAuthTokenGenerator
{
    public String generateAuthToken(
            final String hostname,
            final int port,
            final String username,
            final Region region,
            final AwsCredentialsProvider credentialsProvider)
    {
        Validate.notNull(credentialsProvider, "credentialsProvider must not be null");

        RdsUtilities utilities = RdsUtilities.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        GenerateAuthenticationTokenRequest tokenRequest = GenerateAuthenticationTokenRequest.builder()
                .hostname(hostname)
                .port(port)
                .username(username)
                .credentialsProvider(credentialsProvider)
                .build();
        return utilities.generateAuthenticationToken(tokenRequest);
    }
}
