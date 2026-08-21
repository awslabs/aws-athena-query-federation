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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RdsIamAuthTokenGeneratorTest
{
    private static final int PORT = 5432;
    private static final String USERNAME = "athena_iam";
    private static final String SIGNED_TOKEN = "signed-token";

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Test
    public void generateAuthToken_WhenInputValid_ReturnsGeneratedToken()
    {
        RdsUtilities rdsUtilities = mock(RdsUtilities.class);
        RdsUtilities.Builder utilitiesBuilder = mock(RdsUtilities.Builder.class);

        when(utilitiesBuilder.region(any(Region.class))).thenReturn(utilitiesBuilder);
        when(utilitiesBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(utilitiesBuilder);
        when(utilitiesBuilder.build()).thenReturn(rdsUtilities);
        when(rdsUtilities.generateAuthenticationToken(any(GenerateAuthenticationTokenRequest.class))).thenReturn(SIGNED_TOKEN);

        try (MockedStatic<RdsUtilities> rdsUtilitiesStatic = mockStatic(RdsUtilities.class)) {
            rdsUtilitiesStatic.when(RdsUtilities::builder).thenReturn(utilitiesBuilder);

            String token = new RdsIamAuthTokenGenerator().generateAuthToken(
                    "mydb.us-east-1.rds.amazonaws.com",
                    PORT,
                    USERNAME,
                    Region.US_EAST_1,
                    awsCredentialsProvider);

            assertEquals(SIGNED_TOKEN, token);
        }
    }

    @Test(expected = NullPointerException.class)
    public void generateAuthToken_WhenCredentialsProviderNull_ThrowsException()
    {
        new RdsIamAuthTokenGenerator().generateAuthToken(
                "mydb.us-east-1.rds.amazonaws.com", PORT, USERNAME, Region.US_EAST_1, null);
    }
}
