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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;

import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class CrossAccountCredentialsProviderV2Test
{
    @BeforeClass
    public static void setUp()
    {
        if (System.getProperty("aws.region") == null && System.getenv("AWS_REGION") == null) {
            System.setProperty("aws.region", "us-east-1");
        }
    }

    @Test
    public void testReturnsDefaultProviderWhenNoRoleArn()
    {
        Map<String, String> configOptions = new HashMap<>();

        AwsCredentialsProvider provider = CrossAccountCredentialsProviderV2
                .getCrossAccountCredentialsIfPresent(configOptions, "test-session");

        assertTrue(provider instanceof DefaultCredentialsProvider);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testThrowsAthenaConnectorExceptionForInvalidRoleArn()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("cross_account_role_arn", "arn:aws:iam::000000000000:role/NonExistentRole");

        CrossAccountCredentialsProviderV2.getCrossAccountCredentialsIfPresent(configOptions, "test-session");
    }
}
