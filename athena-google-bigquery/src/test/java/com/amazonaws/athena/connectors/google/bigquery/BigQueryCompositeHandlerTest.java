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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*"})
@PrepareForTest({BigQueryUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class, BigQueryOptions.class, BigQueryOptions.Builder.class})
public class BigQueryCompositeHandlerTest {
    private BigQueryCompositeHandler bigQueryCompositeHandler;
    @Mock
    BigQuery bigQuery;
    @Mock
    private AWSSecretsManager secretsManager;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    @Mock
    private BigQueryOptions bigQueryOptions;
    @Mock
    private BigQueryOptions.Builder builder;

    @BeforeMethod
    public void setUp() {

    }

    static{
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void bigQueryCompositeHandlerTest() throws IOException {
        Exception ex = null;
        try {
            PowerMockito.mockStatic(AWSSecretsManagerClientBuilder.class);
            PowerMockito.when(AWSSecretsManagerClientBuilder.defaultClient()).thenReturn(secretsManager);
            GetSecretValueResult getSecretValueResult = new GetSecretValueResult().withVersionStages(Arrays.asList("v1")).withSecretString("{\n" +
                    "  \"type\": \"service_account\",\n" +
                    "  \"project_id\": \"mockProjectId\",\n" +
                    "  \"private_key_id\": \"mockPrivateKeyId\",\n" +
                    "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nmockPrivateKeydsfhdskfhjdfjkdhgfdjkghfdngvfkvfnjvfdjkg\\n-----END PRIVATE KEY-----\\n\",\n" +
                    "  \"client_email\": \"mockabc@mockprojectid.iam.gserviceaccount.com\",\n" +
                    "  \"client_id\": \"000000000000000000000\"\n" +
                    "}");
            Mockito.when(secretsManager.getSecretValue(Mockito.any())).thenReturn(getSecretValueResult);
            PowerMockito.mockStatic(ServiceAccountCredentials.class);
            PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);

            PowerMockito.mockStatic(System.class);
            PowerMockito.when(System.getenv(anyString())).thenReturn("test");

            PowerMockito.mockStatic(BigQueryOptions.Builder.class);
            PowerMockito.when(builder.build()).thenReturn(bigQueryOptions);
            PowerMockito.when(bigQueryOptions.getService()).thenReturn(bigQuery);

            bigQueryCompositeHandler = new BigQueryCompositeHandler();
        }catch (Exception e){
            ex = e;
        }
        assertEquals(null,ex);
    }
}
