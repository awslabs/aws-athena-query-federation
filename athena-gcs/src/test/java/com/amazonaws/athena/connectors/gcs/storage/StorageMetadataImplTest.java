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
package com.amazonaws.athena.connectors.gcs.storage;


import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connectors.gcs.GcsSchemaUtils;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageMetadataConfig;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*", "javax.security.*"})
@PrepareForTest({StorageOptions.class, StorageDatasourceFactory.class, GoogleCredentials.class, GcsSchemaUtils.class, AWSSecretsManagerClientBuilder.class, ServiceAccountCredentials.class, AWSGlueClientBuilder.class, GlueMetadataHandler.class})
public class StorageMetadataImplTest {
    private StorageMetadataImpl storageMetadata;
    private StorageMetadataConfig storageMetadataConfig;
    private List<Blob> blobList;
    private FileSystemDatasetFactory fileSystemDatasetFactory;
    @Mock
    private Page<Blob> blobPage;
    @Mock
    GoogleCredentials credentials;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;
    @Before
    public void setUp() throws Exception
    {
        Storage storage = mock(Storage.class);
        Blob blob = mock(Blob.class);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        PowerMockito.when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        PowerMockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        PowerMockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        PowerMockito.when(mockedOptions.getService()).thenReturn(storage);
        PowerMockito.when(storage.list(any(),any())).thenReturn(blobPage);
        blobList = ImmutableList.of(blob);
        PowerMockito.when(blob.getName()).thenReturn("birthday.parquet");
        PowerMockito.when(blobPage.iterateAll()).thenReturn(blobList);
        storageMetadataConfig = mock(StorageMetadataConfig.class);
        String gcsJson = "{\"gcs_credential_keys\":\"{\\\"type\\\": \\\"service_account\\\",\\\"project_id\\\": \\\"athena-federated-query\\\",\\\"private_key_id\\\": \\\"6d559a25a53c802bf466f0dc98ae5666e6458957\\\",\\\"private_key\\\": \\\"-----BEGIN PRIVATE KEY-----\\\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDM0Ti6m7hZfwNC\\\\nLGTRf3Bk3J8ZvJlnxhQX012shcYXjeIbzNDTq6NREOi8Tk/7WeJK27uX5ZaC128C\\\\nFafUYQL2iX9FVXOMkPRpclTZdpcNihuBkwvZgHuP6Cy/RKfge+YjvE+YJ51Mm+RX\\\\nz+xJj5FBXClVzw7rbV8VZMbbLeAP6kskRZ2jhp3e9g5DSxQNQ7R8K3XC6iskzBET\\\\nfxfETYvQMq3FOJlih98njHEeICfQcMpuPZQykFzxqo587P/67devR+KfHS5RvE89\\\\nieeOnQaImL1R+8J6ZoLKEyLixpZIxLnQEa/NPzPhGHh/783tX/CLTvIbQb5sNhD1\\\\n0S5sYrslAgMBAAECggEAC09mZY+hz3dfE7Fl49e4uikgtAghJITgqSwn2RYOCVDx\\\\ncn2N7pJk014uq+9bJVMiVuXpZwrrk9AXtjLB8M1mn3yaRZbhaZq7lHMW0mdlEf9V\\\\nY4NePSWGLT0x82H0L0ZIQCLu9kUBv7BAHMVDlBBUghp3weBMP53riT/mZ0YSQG6j\\\\nTplLKIztTFugjd7gz+eihUGgYpzzvpFVd8EMXm6rSkkG31s0AJpFSRH47q/XatDp\\\\nhOBYXBpkE47UUfQZ+XFvnyBYkPJE0M67cS9AfUMWAT3Ja4kcjtsd9IjzrNtZGGES\\\\nfOynauyat75v6O3ktuxj3xBWIgztZafHqiAEJihbxQKBgQD7qIp0FSsYEAvYBXIW\\\\nCj9j7pFy8jMl9F48kSgfFSIGyg+wfQhm6pcqp5MQ3sHQgyLtWyckYqtzkH4qN4DN\\\\nHjTwsHgzhr14t3IpN1pRLDM7Xlem/iFMCqSRhuRefbGGp7R79wTT+p0yegPPy96L\\\\noRV5Ake9XvEPTybebkvoB5QPtwKBgQDQWc4jIUYZjy94arxLjkSV0CfLez1lkWes\\\\nZQSNn78ZJu/DmoWNYVsIKJhAuPuVbxfwDBgeJiQjdXtX+mkpfH1oQ6QztTYPybdH\\\\nfTA/RRZN3YS8idwDIuN8SWoH0ky8LYlsv0qox6q7RuJRdTIJJ6QtTJp31qAXRN9J\\\\nB9yLxnXUAwKBgCwNRK8tRW3g4THfQik5gf8sM6m7W9i4/gX8ItnOCTbHCBgMyvu4\\\\n9N0ymDvLwoGNcv+5hRCJdRm1oWAidxlwwDOhsGjUnTYUZpdwaQ7sfctWqGFC+vEk\\\\nq8oNoswnlHvNv3ozs1Sn+fxr/17QgRRQDkIwc/5iMDBN2q/0/rSPHwvhAoGBAIDI\\\\nkdc7EPZpgAEhkIPvS0uC36Yx5hBq4Tc5NahIrrlgTOGOGLD8FWWkmf+fl7qVcalq\\\\nAFpaXwof6v7FhM0k6utQrCVeBC2cFJK2ueTR0miM3Sgg2oKBxTUkt8pf3hiO2RwD\\\\n3aUXzdt2hBuvoh7whtWNPQmH+2qGorGkj1lCccB7AoGAWTEYvxG8BsQqk54ZtGxB\\\\n8BBv5y3rTypEMeGVVZhXA9Bf0U17J8gl4UMjsr3UZJkKqm91xBUCQId7nefDxoHi\\\\nLxARLgrVv9plhFGKRt9oriO/jEe2cd5pcgAspCc3jNERn4uUhQOTEjqUZE+F3th6\\\\n3cBBBa/2Bouf76nUmf91Ptg=\\\\n-----END PRIVATE KEY-----\\\\n\\\",\\\"client_email\\\": \\\"akshay-kachore246369-trianz-co@athena-federated-query.iam.gserviceaccount.com\\\",\\\"client_id\\\": \\\"109479973374724829513\\\",\\\"auth_uri\\\": \\\"https://accounts.google.com/o/oauth2/auth\\\",\\\"token_uri\\\": \\\"https://oauth2.googleapis.com/token\\\",\\\"auth_provider_x509_cert_url\\\": \\\"https://www.googleapis.com/oauth2/v1/certs\\\",\\\"client_x509_cert_url\\\": \\\"https://www.googleapis.com/robot/v1/metadata/x509/akshay-kachore246369-trianz-co%40athena-federated-query.iam.gserviceaccount.com\\\"}\",\"gcs_HMAC_key\":\"GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NXL5GOSRX6BA2F7RMA6YJGO3Q\",\"gcs_HMAC_secret\":\"haK0skzuPrUljknEsfcRJCYRXklVAh+LuaIiirh1\"}";
        Map<String, String> properties = new HashMap<>();
        properties.put("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/service-account.json");
        properties.put("SSL_CERT_FILE", "/tmp/cacert.pem");
        properties.put("gcs_credential_key", "gcs_credential_keys");
        properties.put("gcs_secret_name", "gcs-athena");
        when(storageMetadataConfig.credentialsJson()).thenReturn(gcsJson);
        when(storageMetadataConfig.properties()).thenReturn(properties);
        mockStatic(ServiceAccountCredentials.class);
        PowerMockito.when(ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        mockStatic(GoogleCredentials.class);
        PowerMockito.when(GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        PowerMockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        storageMetadata = new StorageMetadataImpl(storageMetadataConfig);
    }
    @Test
    @Ignore
    public void testGetStorageTable() throws Exception {
        fileSystemDatasetFactory = mock(FileSystemDatasetFactory.class);
        PowerMockito.whenNew(FileSystemDatasetFactory.class).withArguments(any(),any(),any(),any()).thenReturn(fileSystemDatasetFactory);
        storageMetadata.getStorageTable("mydatalake1", "birthday", "parquet");
    }
}
