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

import com.amazonaws.athena.storage.AbstractStorageDatasource;
import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.StorageDatasource;
import com.amazonaws.athena.storage.common.StorageObjectSchema;
import com.amazonaws.athena.storage.common.StoragePartition;
import com.amazonaws.athena.storage.gcs.GroupSplit;
import com.amazonaws.athena.storage.gcs.ParquetUtil;
import com.amazonaws.athena.storage.gcs.StorageSplit;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.amazonaws.athena.storage.mock.GcsReadRecordsRequest;
import com.amazonaws.util.ValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.net.URL;
import java.util.List;

import static com.amazonaws.athena.storage.StorageConstants.FILE_EXTENSION_ENV_VAR;
import static org.apache.arrow.vector.types.Types.MinorType.DATEDAY;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({StorageOptions.class, FilterCompat.class, ParquetUtil.class, StorageFile.class, AbstractStorageDatasource.class, ObjectMapper.class, SeekableInputStream.class, ParquetFileReader.class, GoogleCredentials.class})
public class ParquetDatasourceTest extends GcsTestBase
{

    @Mock
    ParquetFileReader parquetFileReader;

    private ParquetDatasource parquetDatasource;

    @Before
    public void setUp() throws Exception
    {
        setUpBeforeClass();
        suppress(constructor(AbstractStorageDatasource.class));
        suppress(constructor(ParquetFileReader.class, InputFile.class, ParquetReadOptions.class));
        ObjectMapper objectMapper = PowerMockito.mock(ObjectMapper.class);
        PowerMockito.whenNew(ObjectMapper.class).withNoArguments().thenReturn(objectMapper);
        PowerMockito.when(objectMapper.readValue(anyString(), eq(StorageSplit.class))).thenReturn(mock(StorageSplit.class));
        suppress(constructor(AbstractStorageDatasource.class));
        parquetDatasource = mockParquetDatasource();

    }

    @Test
    @Ignore
    public void testGetStorageSplits() throws Exception
    {
        PowerMockito.mockStatic(ParquetFileReader.class);
        Storage st = mock(Storage.class);
        PowerMockito.when(st.get(Mockito.any(BlobId.class))).thenReturn(mock(Blob.class));
        PowerMockito.when(st.reader(Mockito.any(BlobId.class))).thenReturn(mock(ReadChannel.class));
        URL resource = getClass().getClassLoader().getResource(PARQUET_FILE);
        assertNotNull(resource, "Input resource was null");
        StorageFile cache = new StorageFile()
                .bucketName(BUCKET)
                .fileName(PARQUET_FILE)
                .storage(st);
        GcsInputFile gcsInputFile = mock(GcsInputFile.class);
        GcsInputFile gcsInputFile1 = new GcsInputFile(cache);
        mockStatic(FileCacheFactory.class);
        PowerMockito.when(FileCacheFactory.getGCSInputFile(ArgumentMatchers.any(Storage.class), anyString(), anyString())).thenReturn(gcsInputFile);
        PowerMockito.when(gcsInputFile.newStream()).thenReturn(mock(SeekableInputStream.class));
        PowerMockito.whenNew(ParquetFileReader.class).withArguments(gcsInputFile1, ParquetReadOptions.builder().build()).thenReturn(parquetFileReader);

        List<StorageSplit> storageSplits = parquetDatasource.getStorageSplits(BUCKET, CSV_FILE);
        assertNotNull(storageSplits, "Storage split was null");
        assertFalse(storageSplits.isEmpty(), "Storage split was empty");
    }

}
