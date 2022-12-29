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
import com.amazonaws.athena.connectors.gcs.storage.datasource.StorageTable;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
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

import java.util.*;

import static java.util.Arrays.asList;
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
        String gcsJson = "{\"gcs_credential_keys\":\"{\\\"type\\\": \\\"service_account\\\",\\\"project_id\\\": \\\"afq\\\",\\\"private_key_id\\\": \\\"6d559a25a53c666e6123456\\\",\\\"private_key\\\": \\\"-----BEGIN PRIVATE KEY-----\\\\n3cBBBa/2Bouf76nUmf91Ptg=\\\\n-----END PRIVATE KEY-----\\\\n\\\",\\\"client_email\\\": \\\"afq.iam.gserviceaccount.com\\\",\\\"client_id\\\": \\\"10947997337471234567\\\",\\\"auth_uri\\\": \\\"https://accounts.google.com/o/oauth2/auth\\\",\\\"token_uri\\\": \\\"https://oauth2.googleapis.com/token\\\",\\\"auth_provider_x509_cert_url\\\": \\\"https://www.googleapis.com/oauth2/v1/certs\\\",\\\"client_x509_cert_url\\\": \\\"https://www.googleapis.com/robot/v1/metadata/x509/afq.iam.gserviceaccount.com\\\"}\",\"gcs_HMAC_key\":\"GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NX\",\"gcs_HMAC_secret\":\"haK0skzuPrUljknEsfcRJCYR\"}";
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
    public void testGetStorageTable()
    {
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(asList(field), metadataSchema);
        storageMetadata = mock(StorageMetadataImpl.class);
        when(storageMetadata.getStorageTable(any(),any(),any())).thenCallRealMethod();
        when(storageMetadata.getStorageFiles(any(),any(),any())).thenCallRealMethod();
        when(storageMetadata.getFileSchema(any(),any(),any())).thenReturn(schema);
        Optional<StorageTable> storageTable = storageMetadata.getStorageTable("mydatalake1", "birthday", "parquet");
        Assert.assertTrue(storageTable.isPresent());
    }
}
