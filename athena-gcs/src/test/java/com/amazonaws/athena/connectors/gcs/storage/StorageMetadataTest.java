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


import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.gcs.GenericGcsTest;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.gcs.GcsConstants.CLASSIFICATION_GLUE_TABLE_PARAM;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.PARTITION_PATTERN_KEY;
import static com.amazonaws.athena.connectors.gcs.GcsMetadataHandlerTest.LOCATION;
import static com.amazonaws.athena.connectors.gcs.GcsMetadataHandlerTest.PARQUET;
import static com.amazonaws.athena.connectors.gcs.GcsMetadataHandlerTest.TABLE_1;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.allocator;
import static com.amazonaws.athena.connectors.gcs.GcsTestUtils.createColumn;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StorageMetadataTest extends GenericGcsTest
{
    @Mock
    GoogleCredentials credentials;
    @Mock
    Storage storage;
    @Mock
    Blob blob;
    private StorageMetadata storageMetadata;
    private List<Blob> blobList;
    @Mock
    private Page<Blob> blobPage;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    private MockedStatic<StorageOptions> mockedStorageOptions;
    private MockedStatic<ServiceAccountCredentials> mockedServiceAccountCredentials;
    private MockedStatic<GoogleCredentials> mockedGoogleCredentials;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception
    {
        mockedStorageOptions = Mockito.mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        mockedStorageOptions.when(StorageOptions::newBuilder).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        Mockito.when(optionBuilder.setCredentials(ArgumentMatchers.any())).thenReturn(optionBuilder);
        Mockito.when(optionBuilder.build()).thenReturn(mockedOptions);
        Mockito.when(mockedOptions.getService()).thenReturn(storage);
        blobList = ImmutableList.of(blob);

        String gcsJson = "{\"gcs_credential_keys\":\"{\\\"type\\\": \\\"service_account\\\",\\\"project_id\\\": \\\"afq\\\",\\\"private_key_id\\\": \\\"6d559a25a53c666e6123456\\\",\\\"private_key\\\": \\\"-----BEGIN PRIVATE KEY-----\\\\n3cBBBa/2Bouf76nUmf91Ptg=\\\\n-----END PRIVATE KEY-----\\\\n\\\",\\\"client_email\\\": \\\"afq.iam.gserviceaccount.com\\\",\\\"client_id\\\": \\\"10947997337471234567\\\",\\\"auth_uri\\\": \\\"https://accounts.google.com/o/oauth2/auth\\\",\\\"token_uri\\\": \\\"https://oauth2.googleapis.com/token\\\",\\\"auth_provider_x509_cert_url\\\": \\\"https://www.googleapis.com/oauth2/v1/certs\\\",\\\"client_x509_cert_url\\\": \\\"https://www.googleapis.com/robot/v1/metadata/x509/afq.iam.gserviceaccount.com\\\"}\",\"gcs_HMAC_key\":\"GOOG1EGNWCPMWNY5IOMRELOVM22ZQEBEVDS7NX\",\"gcs_HMAC_secret\":\"haK0skzuPrUljknEsfcRJCYR\"}";

        mockedServiceAccountCredentials = Mockito.mockStatic(ServiceAccountCredentials.class);
        mockedServiceAccountCredentials.when(() -> ServiceAccountCredentials.fromStream(Mockito.any())).thenReturn(serviceAccountCredentials);
        MockitoAnnotations.initMocks(this);
        mockedGoogleCredentials = Mockito.mockStatic(GoogleCredentials.class);
        mockedGoogleCredentials.when(() -> GoogleCredentials.fromStream(Mockito.any())).thenReturn(credentials);
        Mockito.when(credentials.createScoped((Collection<String>) any())).thenReturn(credentials);
        storageMetadata = new StorageMetadata(gcsJson);
    }

    @After
    public void tearDown()
    {
        mockedStorageOptions.close();
        mockedServiceAccountCredentials.close();
        mockedGoogleCredentials.close();
    }

    private void storageMock()
    {
        GenericGcsTest.setInternalState(storageMetadata, "storage", storage);
        Mockito.when(storage.list(any(), any())).thenReturn(blobPage);
        blobList = ImmutableList.of(blob);
        Mockito.when(blob.getName()).thenReturn("birthday.parquet");
        Mockito.when(blob.getSize()).thenReturn(10L);
        Mockito.when(blobPage.iterateAll()).thenReturn(blobList);
    }

    @Test
    public void testBuildTableSchema() throws Exception
    {
        Table table = new Table();
        table.setName("birthday");
        table.setDatabaseName("default");
        table.setParameters(ImmutableMap.of("classification", "parquet"));
        table.setStorageDescriptor(new StorageDescriptor()
                .withLocation("gs://mydatalake1test/birthday/"));
        table.setCatalogId("catalog");
        storageMetadata = mock(StorageMetadata.class);
        storageMock();
        when(storageMetadata.buildTableSchema(any(), any())).thenCallRealMethod();
        when(storageMetadata.getAnyFilenameInPath(any(), any())).thenCallRealMethod();
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(asList(field), metadataSchema);
        when(storageMetadata.getFileSchema(any(), any(), any(), any())).thenReturn(schema);
        Schema outSchema = storageMetadata.buildTableSchema(table, allocator);
        assertNotNull(outSchema);
    }

    @Test
    public void testGetPartitionFolders() throws URISyntaxException
    {
        //single partition
        getStorageList(ImmutableList.of("year=2000/birthday.parquet", "year=2000/", "year=2000/birthday1.parquet"));
        AWSGlue glue = Mockito.mock(AWSGlueClient.class);
        List<Field> fieldList = ImmutableList.of(new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null));
        List<Column> partKeys = ImmutableList.of(createColumn("year", "varchar"));
        Schema schema = getSchema(glue, fieldList, partKeys, "year=${year}/");
        List<Map<String, String>> partValue = storageMetadata.getPartitionFolders(schema, new TableName("testSchema", "testTable"), new Constraints(ImmutableMap.of()), glue);
        assertEquals(1, partValue.size());
        assertEquals(partValue, ImmutableList.of(ImmutableMap.of("year", "2000")));

        //nested partition with folder
        getStorageList(ImmutableList.of("year=2000/birth_month1/birthday.parquet",
                "year=2000/",
                "year=2000/birth_month1/",
                "year=2000/birth_month2/birthday.parquet",
                "year=2000/birth_month2/",
                "year=2001/birth_month1/birthday.parquet",
                "year=2001/",
                "year=2001/birth_month1/",
                "year=2001/birth_month2/birthday.parquet",
                "year=2001/birth_month2/"
        ));
        List<Field> fieldList1 = ImmutableList.of(new Field("year", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("month", FieldType.nullable(new ArrowType.Utf8()), null));
        List<Column> partKeys1 = ImmutableList.of(createColumn("year", "varchar"), createColumn("month", "varchar"));
        Schema schema1 = getSchema(glue, fieldList1, partKeys1, "year=${year}/birth_month${month}/");
        List<Map<String, String>> partValue1 = storageMetadata.getPartitionFolders(schema1, new TableName("testSchema", "testTable"), new Constraints(ImmutableMap.of()), glue);
        assertEquals(4, partValue1.size());
        assertEquals(partValue1, ImmutableList.of(ImmutableMap.of("year", "2000", "month", "1"), ImmutableMap.of("year", "2000", "month", "2"),
                ImmutableMap.of("year", "2001", "month", "1"), ImmutableMap.of("year", "2001", "month", "2")));

        // nested partition without folder
        getStorageList(ImmutableList.of("year=2000/birth_month1/birthday.parquet",
                "year=2000/birth_month2/birthday.parquet",
                "year=2001/birth_month1/birthday.parquet",
                "year=2001/birth_month2/birthday.parquet"
        ));
        List<Field> fieldList2 = ImmutableList.of(new Field("year", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("month", FieldType.nullable(new ArrowType.Utf8()), null));
        List<Column> partKeys2 = ImmutableList.of(createColumn("year", "varchar"), createColumn("month", "varchar"));
        Schema schema2 = getSchema(glue, fieldList2, partKeys2, "year=${year}/birth_month${month}/");
        List<Map<String, String>> partValue2 = storageMetadata.getPartitionFolders(schema2, new TableName("testSchema", "testTable"), new Constraints(ImmutableMap.of()), glue);
        assertEquals(4, partValue2.size());
        assertEquals(partValue2, ImmutableList.of(ImmutableMap.of("year", "2000", "month", "1"), ImmutableMap.of("year", "2000", "month", "2"),
                ImmutableMap.of("year", "2001", "month", "1"), ImmutableMap.of("year", "2001", "month", "2")));

        //partition without file
        getStorageList(ImmutableList.of("year=2000/"));
        List<Field> fieldList3 = ImmutableList.of(new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null));
        List<Column> partKeys3 = ImmutableList.of(createColumn("year", "varchar"));
        Schema schema3 = getSchema(glue, fieldList3, partKeys3, "year=${year}/");
        List<Map<String, String>> partValue3 = storageMetadata.getPartitionFolders(schema3, new TableName("testSchema", "testTable"), new Constraints(ImmutableMap.of()), glue);
        assertEquals(0, partValue3.size());
        assertEquals(partValue3, ImmutableList.of());

        //partition without file/folder
        getStorageList(ImmutableList.of());
        List<Field> fieldList4 = ImmutableList.of(new Field("year", FieldType.nullable(new ArrowType.Int(64, true)), null));
        List<Column> partKeys4 = ImmutableList.of(createColumn("year", "varchar"));
        Schema schema4 = getSchema(glue, fieldList4, partKeys4, "year=${year}/");
        List<Map<String, String>> partValue4 = storageMetadata.getPartitionFolders(schema3, new TableName("testSchema", "testTable"), new Constraints(ImmutableMap.of()), glue);
        assertEquals(0, partValue4.size());
        assertEquals(partValue4, ImmutableList.of());
    }

    @NotNull
    private Schema getSchema(AWSGlue glue, List<Field> fieldList, List<Column> partKeys, String partitionPattern)
    {
        Map<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("dataFormat", "parquet");
        Schema schema = new Schema(fieldList, metadataSchema);
        GetTableResult getTablesResult = new GetTableResult();
        getTablesResult.setTable(new Table().withName(TABLE_1)
                .withParameters(ImmutableMap.of(CLASSIFICATION_GLUE_TABLE_PARAM, PARQUET,
                        PARTITION_PATTERN_KEY, partitionPattern))
                .withPartitionKeys(partKeys)
                .withStorageDescriptor(new StorageDescriptor()
                        .withLocation(LOCATION)));
        Mockito.when(glue.getTable(any())).thenReturn(getTablesResult);
        return schema;
    }

    private void getStorageList(List<String> partitionFiles)
    {
        GenericGcsTest.setInternalState(storageMetadata, "storage", storage);
        Mockito.when(storage.list(any(), any())).thenReturn(blobPage);
        List<Blob> bList = new ArrayList<>();
        for (String fileName : partitionFiles) {
            Blob blob = Mockito.mock(Blob.class);
            Mockito.when(blob.getName()).thenReturn(fileName);
            Long size;
            if (fileName.contains(".")) {
                size = 1L;
            } else {
                size = 0L;
            }
            Mockito.when(blob.getSize()).thenReturn(size);
            bList.add(blob);
        }
        blobList = ImmutableList.copyOf(bList);
        Mockito.when(blobPage.iterateAll()).thenReturn(blobList);
    }
}
