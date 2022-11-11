/*-
 * #%L
 * Amazon Athena Storage API
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
package com.amazonaws.athena.storage.io;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.gcs.GcsCsvSplitUtil;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.gcs.io.GcsOnlineStream;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.amazonaws.athena.storage.mock.GcsReadRecordsRequest;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static com.amazonaws.util.ValidationUtils.assertNotNull;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GoogleCredentials.class, StorageOptions.class})
@Ignore
public class SeekableInputStreamCsvTest extends GcsTestBase
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
    public void testCsvSplitWithInputStream() throws Exception
    {

        String[] fileNames = {CSV_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        RandomAccessFile file = new RandomAccessFile(csvFile, "r");
        Storage storage = mockStorageWithBlobIterator(BUCKET, file.length(), PARQUET_FILE);
        StorageFile storageFile = new StorageFile()
                .storage(storage)
                .bucketName(BUCKET)
                .fileName(PARQUET_FILE);
        mockStatic(FileCacheFactory.class);
        PowerMockito.when(FileCacheFactory.createRandomFile(storage, BUCKET, CSV_FILE)).thenReturn(storageFile);
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(2, fileName, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
    }


    @Test
    public void testCsvSplitWithUsingUtil() throws Exception
    {
        String[] fileNames = {CSV_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        Storage storage = mockStorageWithBlobIterator(BUCKET, csvFile.length(), CSV_FILE);
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(storage)
                .bucketName(BUCKET)
                .fileName(CSV_FILE);
        mockStatic(FileCacheFactory.class);
        PowerMockito.when(FileCacheFactory.createOnlineGcsStream(storage, BUCKET, CSV_FILE)).thenReturn(gcsOnlineStream);
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(2, fileName, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
    }

    @Test
    public void testCsvSplitWithUsingDatasource() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        csvDatasource.loadAllTables(BUCKET);
        List<StorageSplit> splits = csvDatasource.getStorageSplits(BUCKET, CSV_FILE);
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
    }

    @Test
    public void testCsvSplit() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        List<StorageSplit> splits = new ArrayList<>();
        String[] fileNames = {CSV_FILE};
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(99, fileName, 100));
        }
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        csvDatasource.loadAllTables(BUCKET);
        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(Map.of(),
                BUCKET, CSV_TABLE, splits.get(0), false);
        List<StorageSplit> splits1 = csvDatasource.getStorageSplits(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), BUCKET,
                CSV_FILE);
        assertNotNull(splits1, "Spits were null");
        assertFalse(splits1.isEmpty(), "Split was empty");
    }

    @Test
    public void testCsvGetRecords() throws Exception
    {

        mockStorageWithInputStream(BUCKET, CSV_FILE);
        String[] fileNames = {CSV_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(99, fileName, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);

        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(Map.of(),
                BUCKET, CSV_TABLE, splits.get(0), false);
        csvDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        assertFalse(spiller.spilled(), "No records found");
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        csvDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, mockedQueryStatusChecker);
        assertNotNull(spiller, "No records returned");
        assertTrue(spiller.spilled(), "No records found");

    }

    @Test
    public void testCsvGetFilteredRecordsWithEmpIdEqual() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        String[] fileNames = {CSV_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(99, fileName, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("EMPLOYEEID", VARCHAR.getType(), "98"),
                BUCKET, CSV_TABLE, splits.get(0), false);
        csvDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        csvDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, mockedQueryStatusChecker);
        assertNotNull(spiller, "No records returned");
        assertEquals(spiller.getBlock().getRowCount(), 1);
    }

    @Test
    public void testCsvGetFilteredRecordsWithEmpIdIn() throws Exception
    {
        mockStorageWithInputStream(BUCKET, CSV_FILE);
        String[] fileNames = {CSV_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsCsvSplitUtil.getStorageSplitList(99, fileName, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        parquetProps.put(FILE_EXTENSION_ENV_VAR, "csv");
        StorageDatasource csvDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithInClause("EMPLOYEEID", VARCHAR.getType(), List.of("90", "98")),
                BUCKET, CSV_TABLE, splits.get(0), false);
        csvDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        csvDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, mockedQueryStatusChecker);
        assertTrue(spiller.spilled(), "No records found");
    }
}
