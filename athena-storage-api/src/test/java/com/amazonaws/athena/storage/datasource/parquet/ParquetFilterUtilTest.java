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
package com.amazonaws.athena.storage.datasource.parquet;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpiller;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.datasource.ParquetDatasource;
import com.amazonaws.athena.storage.datasource.StorageDatasourceFactory;
import com.amazonaws.athena.storage.gcs.GcsParquetSplitUtil;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.mock.GcsReadRecordsRequest;
import com.amazonaws.util.ValidationUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GoogleCredentials.class, StorageOptions.class, ParquetDatasource.class})
public class ParquetFilterUtilTest extends GcsTestBase
{
    public static final String EMPLOYEE_ID = "EMPLOYEEID";
    static FederatedIdentity federatedIdentity;
    private final String[] fields = {EMPLOYEE_ID, "salary", "NAME", "EMAIL", "ADDRESS1", "ADDRESS2", "CITY", "STATE", "ZIPCODE"};
    ParquetFileReader reader;

    @BeforeClass
    public static void setUp()
    {
        setUpBeforeClass();
        federatedIdentity = mock(FederatedIdentity.class);
    }

    @Before
    public void setUpBeforeTest() throws URISyntaxException, IOException
    {
        URL parquetFileResourceUri = ClassLoader.getSystemResource(PARQUET_FILE_4_STREAM);
        File parquetFile = new File(parquetFileResourceUri.toURI());
        Path path = new Path(parquetFile.getAbsolutePath());
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        reader = new ParquetFileReader(new Configuration(), path, footer);
    }

    @Test
    public void testParquetGetFilteredRecordsWithIdAndNameEqual() throws Exception
    {
        StorageWithInputTest storageWithInput = mockStorageWithInputFile(BUCKET, PARQUET_FILE_4_STREAM);
        String[] fileNames = {PARQUET_FILE_4_STREAM};
        List<StorageSplit> splits = new ArrayList<>();
        for (String fileName : fileNames) {
            splits.addAll(GcsParquetSplitUtil.getStorageSplitList(fileName, reader, 100));
        }
        ValidationUtils.assertNotNull(splits, "Spits were null");
        assertFalse(splits.isEmpty(), "Split was empty");
        PowerMockito.when(FileCacheFactory.getEmptyGCSInputFile(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(storageWithInput.getInputFile());
        PowerMockito.whenNew(ParquetFileReader.class).withAnyArguments().thenReturn(reader);
        StorageDatasource parquetDatasource = StorageDatasourceFactory.createDatasource(gcsCredentialsJson, parquetProps);
        GcsReadRecordsRequest recordsRequest = buildReadRecordsRequest(createMultiFieldSummaryWithLValueRangeEqual(List.of("id", "name"),
                        List.of(BIGINT.getType(), VARCHAR.getType()), List.of(1L, "Azam")),
                BUCKET, "customer_info", splits.get(0), true);
        parquetDatasource.loadAllTables(BUCKET);
        S3BlockSpiller spiller = getS3SpillerObject(recordsRequest.getSchema());
        QueryStatusChecker mockedQueryStatusChecker = mock(QueryStatusChecker.class);
        when(mockedQueryStatusChecker.isQueryRunning()).thenReturn(true);
        parquetDatasource.readRecords(recordsRequest.getSchema(),
                recordsRequest.getConstraints(), recordsRequest.getTableName(), recordsRequest.getSplit(),
                spiller, mockedQueryStatusChecker);
        assertNotNull(spiller.getBlock().getRowCount());
    }
}
