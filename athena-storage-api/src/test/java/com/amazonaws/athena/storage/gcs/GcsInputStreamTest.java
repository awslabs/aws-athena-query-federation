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
package com.amazonaws.athena.storage.gcs;

import com.amazonaws.athena.storage.GcsTestBase;
import com.amazonaws.athena.storage.gcs.io.GcsOfflineStream;
import com.amazonaws.athena.storage.gcs.io.GcsOnlineStream;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;

import static org.junit.Assert.assertTrue;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GoogleCredentials.class, StorageOptions.class})
public class GcsInputStreamTest extends GcsTestBase
{
    @Test
    public void testRead() throws Exception
    {
        URL csvFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(csvFileResourceUri.toURI());
        Storage storage = mockStorageWithBlobIterator(BUCKET, csvFile.length(), CSV_FILE);
        GcsOfflineStream gcsOfflineStream = new GcsOfflineStream()
                .storage(storage)
                .bucketName(BUCKET)
                .fileName(CSV_FILE)
                .file(csvFile)
                .inputStream(new FileInputStream(csvFile));
        GcsInputStream gcsInputStream = new GcsInputStream(gcsOfflineStream);
        int readByte = gcsInputStream.read();
        assertTrue("No bytes read", readByte > -1);
    }

    @Test
    public void testReadWithStorage() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(CSV_FILE);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(parquetFileResourceUri.toURI());
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(CSV_FILE);
        GcsInputStream gcsInputStream = new GcsInputStream(gcsOnlineStream);
        int readByte = gcsInputStream.read();
        assertTrue("No bytes read", readByte > -1);
    }

    @Test
    public void testReadByteWithStorage() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(CSV_FILE);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(parquetFileResourceUri.toURI());
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(CSV_FILE);
        GcsInputStream gcsInputStream = new GcsInputStream(gcsOnlineStream);
        assertTrue("File size is zero", gcsInputStream.getFileSize() != 0);
        int readByte = gcsInputStream.read();
        assertTrue("No bytes read", readByte > -1);
    }

    @Test
    public void testReadBytesWithStorageFromPosition() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(CSV_FILE);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(parquetFileResourceUri.toURI());
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(CSV_FILE);
        GcsInputStream gcsInputStream = new GcsInputStream(gcsOnlineStream);
        byte[] bytes = new byte[4];
        int bytesRead = gcsInputStream.read(bytes, 0, 4);
        assertTrue("No bytes read", bytesRead > 0);
    }

    @Test
    public void testReadBytesWithStorage() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(CSV_FILE);
        URL parquetFileResourceUri = ClassLoader.getSystemResource(CSV_FILE);
        File csvFile = new File(parquetFileResourceUri.toURI());
        GcsOnlineStream gcsOnlineStream = new GcsOnlineStream()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(CSV_FILE);
        GcsInputStream gcsInputStream = new GcsInputStream(gcsOnlineStream);
        byte[] bytes = new byte[4];
        int bytesRead = gcsInputStream.read(bytes);
        assertTrue("No bytes read", bytesRead > 0);
    }
}
