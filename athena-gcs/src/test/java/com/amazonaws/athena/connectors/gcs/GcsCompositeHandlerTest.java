/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@TestInstance(PER_CLASS)
public class GcsCompositeHandlerTest extends GenericGcsTest {

    private SecretsManagerClient secretsManager;
    private ServiceAccountCredentials serviceAccountCredentials;
    private GoogleCredentials credentials;

    @BeforeAll
    public void init() {
        super.initCommonMockedStatic();
        secretsManager = Mockito.mock(SecretsManagerClient.class);
        mockedSecretManagerBuilder.when(SecretsManagerClient::create).thenReturn(secretsManager);
        serviceAccountCredentials = Mockito.mock(ServiceAccountCredentials.class);
        mockedServiceAccountCredentials.when(() -> ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        credentials = Mockito.mock(GoogleCredentials.class);
        mockedGoogleCredentials.when(() -> GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        S3Client mockedAmazonS3 = Mockito.mock(S3Client.class);
        when(S3Client.create()).thenReturn(mockedAmazonS3);
    }

    @AfterAll
    public void cleanUp() {
        super.closeMockedObjects();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGcsCompositeHandler() throws IOException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException
    {
        GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder()
                .versionStages(com.google.common.collect.ImmutableList.of("v1"))
                .secretString("{\"gcs_credential_keys\": \"test\"}")
                .build();
        when(secretsManager.getSecretValue(Mockito.isA(GetSecretValueRequest.class))).thenReturn(getSecretValueResponse);
        when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        GcsCompositeHandler gcsCompositeHandler = new GcsCompositeHandler();
        assertTrue(gcsCompositeHandler instanceof GcsCompositeHandler);
    }
}
