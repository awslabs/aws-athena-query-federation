/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;


@RunWith(MockitoJUnitRunner.class)
public class BigQueryCompositeHandlerTest
{
    static {
        System.setProperty("aws.region", "us-east-1");
    }

    MockedStatic<AWSSecretsManagerClientBuilder> awsSecretManagerClient;
    MockedStatic<ServiceAccountCredentials> serviceAccountCredentialsStatic;
    MockedStatic<BigQueryUtils> bigQueryUtils;
    private BigQueryCompositeHandler bigQueryCompositeHandler;
    @Mock
    private AWSSecretsManager secretsManager;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    @Before
    public void setUp()
    {
        bigQueryUtils = mockStatic(BigQueryUtils.class);
        serviceAccountCredentialsStatic = mockStatic(ServiceAccountCredentials.class);
        awsSecretManagerClient = mockStatic(AWSSecretsManagerClientBuilder.class);
    }

    @After
    public void cleanup()
    {
        awsSecretManagerClient.close();
        serviceAccountCredentialsStatic.close();
        bigQueryUtils.close();
    }

    @Test
    public void bigQueryCompositeHandlerTest() throws IOException
    {
        Exception ex = null;

        Mockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
        GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(Arrays.asList("v1")).withSecretString("{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"mockProjectId\",\n" +
                "  \"private_key_id\": \"mockPrivateKeyId\",\n" +
                "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nmockPrivateKeydsfhdskfhjdfjkdhgfdjkghfdngvfkvfnjvfdjkg\\n-----END PRIVATE KEY-----\\n\",\n" +
                "  \"client_email\": \"mockabc@mockprojectid.iam.gserviceaccount.com\",\n" +
                "  \"client_id\": \"000000000000000000000\"\n" +
                "}");

        Mockito.when(ServiceAccountCredentials.fromStream(any())).thenReturn(serviceAccountCredentials);
        bigQueryCompositeHandler = new BigQueryCompositeHandler();
        assertTrue(bigQueryCompositeHandler instanceof BigQueryCompositeHandler);
    }
}
