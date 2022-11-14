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
package com.amazonaws.athena.storage.datasource;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.storage.gcs.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.StorageTable;
import com.amazonaws.athena.storage.gcs.GcsParquetSplitUtil;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.mock.AthenaReadRecordsRequest;
import com.amazonaws.util.ValidationUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.pig.convert.DecimalUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.util.ValidationUtils.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({FileCacheFactory.class, GoogleCredentials.class, StorageOptions.class, FileUtils.class,
        ParquetDatasource.class, DecimalUtils.class})
public class ParquetReadFieldsTest extends GcsTestBase
{
    @Rule
    public PowerMockRule rule = new PowerMockRule();

    @BeforeClass
    public static void setUpBeforeAllTests()
    {
        setUpBeforeClass();
    }

    @Test
    public void testParquetGetStorageTableWithFields() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE_4_DATATYPE_TEST);
        assertNotNull(reader, "Reader was null");
        StorageWithInputTest storageWithInput = mockStorageWithEmptyInputFile(BUCKET, PARQUET_FILE_4_DATATYPE_TEST);
        String[] fileNames = {PARQUET_FILE_4_DATATYPE_TEST};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        mockStatic(GoogleCredentials.class);
        when(GoogleCredentials.fromStream(any())).thenReturn(credentials);
        when(credentials.createScoped(ArgumentMatchers.<List<String>>any())).thenReturn(credentials);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        when(optionBuilder.setCredentials(any())).thenReturn(optionBuilder);
        when(optionBuilder.build()).thenReturn(mockedOptions);
        when(mockedOptions.getService()).thenReturn(storageWithInput.getStorage());

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        Optional<StorageTable> optionalStorageTable = parquetDatasource.getStorageTable(BUCKET, "data_type_test");
        assertTrue(optionalStorageTable.isPresent(), "StorageTable was not found");
    }

    @Test
    public void testParquetGetStorageTableDataTypes() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE);
        assertNotNull(reader, "Reader was null");
        StorageWithInputTest storageWithInput = mockStorageWithEmptyInputFile(BUCKET, PARQUET_FILE);
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        mockStatic(GoogleCredentials.class);
        when(GoogleCredentials.fromStream(any())).thenReturn(credentials);
        when(credentials.createScoped(ArgumentMatchers.<List<String>>any())).thenReturn(credentials);
        mockStatic(StorageOptions.class);
        StorageOptions.Builder optionBuilder = mock(StorageOptions.Builder.class);
        when(StorageOptions.newBuilder()).thenReturn(optionBuilder);
        StorageOptions mockedOptions = mock(StorageOptions.class);
        when(optionBuilder.setCredentials(any())).thenReturn(optionBuilder);
        when(optionBuilder.build()).thenReturn(mockedOptions);
        when(mockedOptions.getService()).thenReturn(storageWithInput.getStorage());

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        Optional<StorageTable> optionalStorageTable = parquetDatasource.getStorageTable(BUCKET, PARQUET_TABLE);
        assertFalse(optionalStorageTable.isEmpty(), "StorageTable was not found");
    }

    @Test
    public void testParquetGetRecordsWithSupportedTypes() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE_4_DATATYPE_TEST);
        String[] fileNames = {PARQUET_FILE_4_DATATYPE_TEST};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }

        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithSummaryRangeValue(
                        "salary", Types.MinorType.FLOAT8.getType(), 2000.00, 3500.00),
                BUCKET, PARQUET_TABLE_4, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertTrue(spillObj.spilled(), "No records returned");
    }

    @Test
    public void testDataTypes() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE);
        assertNotNull(reader, "Reader was null");
        StorageWithInputTest storageWithInput = mockStorageWithInputFile(BUCKET, PARQUET_FILE);
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        ValidationUtils.assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        PowerMockito.when(FileCacheFactory.getEmptyGCSInputFile(any(), any(), any())).thenReturn(storageWithInput.getInputFile());
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(Map.of(), BUCKET, "customer_info", splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, mockedQueryStatusChecker);
        assertTrue(spiller.spilled());
    }

    /**
     * While reading the value, if the field value is null, it throws an exception. However, it just ignores and keep the
     * field value empty (null)
     * @throws Exception If occurs any
     */
    @Test
    public void testDataTypesWithNullPointerException() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE_4_DATATYPE_TEST);
        String[] fileNames = {PARQUET_FILE_4_DATATYPE_TEST};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithSummaryRangeValue(
                        "salary", Types.MinorType.FLOAT8.getType(), 2000.00, 3500.00),
                BUCKET, PARQUET_TABLE_4, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        mockStatic(DecimalUtils.class);
        when(DecimalUtils.binaryToDecimal(any(Binary.class), anyInt(), anyInt())).thenThrow(new NullPointerException());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertTrue(spillObj.spilled(), "No records returned");
    }

    /**
     * While reading the value, if the field value is null, it throws an exception. However, it just ignores and keep the
     * field value empty (null)
     * @throws Exception If occurs any
     */
    @Test
    public void testDataTypesWithException() throws Exception
    {
        ParquetFileReader reader = getMockedReader(BUCKET, PARQUET_FILE_4_DATATYPE_TEST);
        String[] fileNames = {PARQUET_FILE_4_DATATYPE_TEST};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithSummaryRangeValue(
                        "salary", Types.MinorType.FLOAT8.getType(), 2000.00, 3500.00),
                BUCKET, PARQUET_TABLE_4, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        mockStatic(DecimalUtils.class);
        when(DecimalUtils.binaryToDecimal(any(Binary.class), anyInt(), anyInt())).thenThrow(new UnsupportedOperationException());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertTrue(spillObj.spilled(), "No records returned");
    }

    // helpers
    private ParquetFileReader getMockedReader(String bucket, String fileName) throws Exception
    {
        StorageWithInputTest storageWithInput = mockStorageWithInputFile(bucket, fileName);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(fileName);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        Path path = new Path(parquetFile.getAbsolutePath());
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        ParquetFileReader reader = new ParquetFileReader(new Configuration(), path, footer);
        PowerMockito.when(FileCacheFactory.getEmptyGCSInputFile(any(), any(), any())).thenReturn(storageWithInput.getInputFile());
        PowerMockito.whenNew(ParquetFileReader.class).withAnyArguments().thenReturn(reader);
        return reader;
    }
}
