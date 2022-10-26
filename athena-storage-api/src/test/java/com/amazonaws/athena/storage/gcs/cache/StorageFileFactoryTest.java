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
package com.amazonaws.athena.storage.gcs.cache;

import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import com.amazonaws.athena.storage.gcs.io.GcsOnlineStream;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({ByteBuffer.class})
public class StorageFileFactoryTest extends GcsTestBase
{

    @Test(expected = RuntimeException.class)
    public void testAnExceptionWhenCreateBytes() throws IOException, URISyntaxException
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE, "parquet");
        mockStatic(FileUtils.class);
        PowerMockito.doThrow(new IOException()).when(FileUtils.class);
        StorageFile storageFile = FileCacheFactory.createRandomFile(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE);
        assertNotNull(storageFile, "File cached was null");
    }

    @Test
    public void testCacheableGetGCSFileCache() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE, "parquet");
        StorageFile storageFile = FileCacheFactory.createRandomFile(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE);
        assertNotNull(storageFile, "File cached was null");
    }

    @Test
    public void testGetGCSFileCache() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE, "cache");
        StorageFile storageFile = FileCacheFactory.createRandomFile(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE);
        assertNotNull(storageFile, "File cached was null");
    }

    @Test
    public void testCacheableGCSInputFile() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(CSV_FILE, "parquet");
        GcsInputFile inputFile = FileCacheFactory.getGCSInputFile(cacheFactoryInfo.getStorage(), BUCKET, CSV_FILE);
        assertNotNull(inputFile, "GCS input file was null");
    }

    @Test
    public void testGetGCSInputFile() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(CSV_FILE, "cache");
        GcsInputFile inputFile = FileCacheFactory.getGCSInputFile(cacheFactoryInfo.getStorage(), BUCKET, CSV_FILE);
        assertNotNull(inputFile, "GCS input file was null");
    }

    @Test
    public void testCacheableGCSInputStream() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE_4_STREAM, "parquet");
        GcsOnlineStream gcsOnlineStream = FileCacheFactory.createOnlineGcsStream(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE_4_STREAM);
        assertTrue("Stream length was zero", gcsOnlineStream.getLength() > 0);
        assertNotNull(gcsOnlineStream, "GCS input stream was null");
    }

    @Test
    public void testGetGCSInputStream() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE_4_STREAM, "cache");
        GcsOnlineStream gcsOnlineStream = FileCacheFactory.createOnlineGcsStream(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE_4_STREAM);
        assertNotNull(gcsOnlineStream, "GCS input stream was null");
    }

    @Test
    public void testGetGCSEmptyStream() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactory(PARQUET_FILE_4_STREAM, "cache");
        GcsInputFile inputFile = FileCacheFactory.getEmptyGCSInputFile(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE_4_STREAM);
        assertNotNull(inputFile, "GCS input file was null");
    }
}
