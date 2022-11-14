/*-
 * #%L
 * athena-hive
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
package com.amazonaws.athena.storage.datasource;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.StorageTable;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.datasource.exception.TableNotFoundException;
import com.amazonaws.athena.storage.gcs.GcsCsvSplitUtil;
import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.cache.CustomGcsReadChannel;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.mock.GcsReadRecordsRequest;
import com.amazonaws.util.ValidationUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static com.amazonaws.util.ValidationUtils.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({ GcsCsvSplitUtil.class, CsvDatasource.class, SeekableGcsInputStream.class,
        GoogleCredentials.class, StorageOptions.class, AbstractStorageDatasource.class})
public class CsvDatasourceTest extends GcsTestBase
{

    static File csvFile;

    @BeforeClass
    public static void setUpBeforeAllTests() throws URISyntaxException
    {
        setUpBeforeClass();
        URL csvFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        csvFile = new File(csvFileResourceUri.toURI());
    }

    @Test
    public void testGetObjectSchema() throws Exception
    {
        StorageWithStreamTest mockStorageWithInputStream = mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        StorageObjectSchema storageObjectSchema = csvDatasource.getObjectSchema(BUCKET, CSV_FILE);
        ValidationUtils.assertNotNull(storageObjectSchema, "Schema were null");
    }

    @Test
    @Ignore
    public void testGetSplitsByStoragePartition() throws Exception
    {
//        StorageWithStreamTest mockStorageWithInputStream = mockStorageWithInputStream(BUCKET, CSV_FILE);
//        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
//        StoragePartition partition = StoragePartition.builder().bucketName("test").location("test").objectNames(List.of("test")).recordCount(10L).children(List.of()).build();
//        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
//        List<StorageSplit> splits = csvDatasource.getSplitsByBucketPrefix(BUCKET, "partition", true, new Constraints(Map.of()));
//        ValidationUtils.assertNotNull(splits, "Split were null");
    }

    @Test
    public void testCsvSplitWithUsingDatasource() throws Exception
    {
        StorageWithStreamTest mockStorageWithInputStream = mockStorageWithInputStream(BUCKET, CSV_FILE);
        ReadChannel channel = new CustomGcsReadChannel(csvFile);
        doReturn(channel).when(mockStorageWithInputStream.getStorage()).reader(any(BlobId.class));
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        List<StorageSplit> splits = csvDatasource.getStorageSplits(BUCKET, CSV_FILE);
        ValidationUtils.assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
    }

    @Test
    public void testGetTableFields() throws Exception
    {
        StorageWithStreamTest storageWithStreamTest = mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        ReadChannel ch = mock(ReadChannel.class);
        Storage st = storageWithStreamTest.getStorage();
        PowerMockito.when(st.reader(any())).thenReturn(ch);
        PowerMockito.when(ch.read(any())).thenReturn(1);
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        Optional<StorageTable> obj = csvDatasource.getStorageTable("test", "dimeemployee");
        assertFalse(obj.isEmpty(), "Storage table was empty");
    }

    @Test
    public void testGetStorageTable() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        Optional<StorageTable> obj = csvDatasource.getStorageTable("test", "dimeemployee");
        assertFalse(obj.isEmpty(), "Storage table was not empty");
    }

    @Test(expected = Exception.class)
    public void testCsvGetRecordsException() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(Map.of(),
                BUCKET, CSV_TABLE, new StorageSplit(), false);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        assertFalse(spiller.spilled(), "No records found");

        csvDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, any(QueryStatusChecker.class));

        assertNotNull(spiller, "No records returned");
        assertTrue(spiller.spilled(), "No records found");
    }

}