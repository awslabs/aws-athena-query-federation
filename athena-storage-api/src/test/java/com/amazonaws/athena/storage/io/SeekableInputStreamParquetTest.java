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
import com.amazonaws.athena.storage.gcs.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.datasource.ParquetDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.gcs.GcsParquetSplitUtil;
import com.amazonaws.athena.storage.gcs.GroupSplit;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.mock.AthenaReadRecordsRequest;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.arrow.vector.types.Types;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.*;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.util.ValidationUtils.assertNotNull;
import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({FileCacheFactory.class, GoogleCredentials.class, StorageOptions.class, FileUtils.class, ParquetDatasource.class})
public class SeekableInputStreamParquetTest extends GcsTestBase
{
    static File parquetFile;
    static Path path;
    static ParquetFileReader reader;
    @Rule
    public PowerMockRule rule = new PowerMockRule();

    @BeforeClass
    public static void setUpBeforeAllTests()
    {
        setUpBeforeClass();
    }

    @Before
    public void setUpBeforeTest() throws Exception
    {
        StorageWithInputTest storageWithInput = mockStorageWithInputFile(BUCKET, PARQUET_FILE);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(PARQUET_FILE_4_STREAM);
        parquetFile = new File(parquetFileResourceUri.toURI());
        path = new Path(parquetFile.getAbsolutePath());
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        reader = new ParquetFileReader(new Configuration(), path, footer);
        PowerMockito.when(FileCacheFactory.getEmptyGCSInputFile(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(storageWithInput.getInputFile());
        PowerMockito.whenNew(ParquetFileReader.class).withAnyArguments().thenReturn(reader);
    }

    @Test
    public void testParquetSplitWithInputStream()
    {
        String[] fileNames = {PARQUET_FILE_4_STREAM};
        List<StorageSplit> splits = new ArrayList<>();

        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
    }

    @Test
    @Ignore
    public void testGetSplitsByStoragePartition() throws Exception
    {
//        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
//        StoragePartition partition = StoragePartition.builder().bucketName("test").location("test").objectNames(List.of("test")).recordCount(10L).children(List.of()).build();
//        parquetDatasource.loadAllTables(BUCKET);
//        List<StorageSplit> splits = parquetDatasource.getSplitsByBucketPrefix(partition, true, BUCKET);
//        assertNotNull(splits, "No splits returned");
//        assertFalse(splits.isEmpty(), "No splits found");
    }

    @Test
    public void testGetObjectSchema() throws Exception
    {
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        StorageObjectSchema storageObjectSchema = parquetDatasource.getObjectSchema(BUCKET, PARQUET_FILE);
        assertNotNull(storageObjectSchema, "No schema returned");
    }

    @Test
    public void testParquetGetRecords() throws Exception
    {
        String[] fileNames = {PARQUET_FILE_4_STREAM};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }

        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");

        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithSummaryRangeValue("salary",
                        Types.MinorType.FLOAT8.getType(), 2000.00, 3500.00),
                BUCKET, PARQUET_TABLE_4, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertEquals(spillObj.getBlock().getRowCount(), 1);
    }

    @Test
    public void testParquetGetFilteredRecordsWithDateEqual() throws Exception
    {
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("dob", DATEDAY.getType(), 5479),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertEquals(spillObj.getBlock().getRowCount(), 1);
    }

    @Test
    public void testParquetGetFilteredRecordsWithNameEqual() throws Exception
    {
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("name", VARCHAR.getType(), "Azam"),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertEquals(spillObj.getBlock().getRowCount(), 1);
    }

    @Test
    public void testParquetGetFilteredRecordsWithIdEqual() throws Exception
    {
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("id", BIGINT.getType(), 1L),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertEquals(spillObj.getBlock().getRowCount(), 1);
    }

    @Test
    public void testParquetGetFilteredRecordsWithSalaryEqual() throws Exception
    {
        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithLValueRangeEqual("salary", FLOAT8.getType(), 3200.5),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mockedQueryStatusChecker);
        assertEquals(spillObj.getBlock().getRowCount(), 1);
    }

    // test with IN
    @Test
    public void testParquetGetFilteredRecordsWithDateIn() throws Exception
    {

        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithInClause("dob", DATEDAY.getType(), List.of(5479, 7305)),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mock(QueryStatusChecker.class));
        assertNotNull(spillObj, "No records returned");
    }

    @Test
    public void testParquetGetFilteredRecordsWithNameIn() throws Exception
    {

        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithInClause("name",
                VARCHAR.getType(), List.of("Azam", "Gaurav")), BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mock(QueryStatusChecker.class));
        assertNotNull(spillObj, "No records returned");
    }

    @Test
    public void testParquetGetFilteredRecordsWithSalaryIn() throws Exception
    {

        String[] fileNames = {PARQUET_FILE};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        AthenaReadRecordsRequest recordsRequest = buildReadRecordsRequest(createSummaryWithInClause("salary", FLOAT8.getType(), List.of(3200.5, 1200.5)),
                BUCKET, PARQUET_TABLE, splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spillObj = getS3SpillerObject(recordsRequest.getSchema());
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spillObj, mock(QueryStatusChecker.class));
        assertNotNull(spillObj, "No records returned");
    }

}
