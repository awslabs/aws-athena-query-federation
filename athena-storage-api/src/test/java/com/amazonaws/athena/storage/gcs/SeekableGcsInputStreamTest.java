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

import com.amazonaws.athena.storage.gcs.io.FileCacheFactory;
import com.amazonaws.athena.storage.gcs.io.GcsInputFile;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.testng.Assert.assertNotNull;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"})
@PrepareForTest({GoogleCredentials.class, StorageOptions.class})
public class SeekableGcsInputStreamTest extends GcsTestBase
{
    @Rule
    public PowerMockRule rule = new PowerMockRule();
    static SeekableGcsInputStream seekableGCSInputStream;
    static Storage st;
    static StorageFile cache;

    @Test
    public void testReadWithStorage() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(PARQUET_FILE_4_EMPTY_STREAM);
        StorageFile storageFile = new StorageFile()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(PARQUET_FILE_4_EMPTY_STREAM);
        int fileLength = (int) cacheFactoryInfo.getTmpFile().length();
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageFile, fileLength);
        inputStream.read();
    }

    @Test
    public void testReadFullyByteBufferWithoutFile() throws Exception
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(PARQUET_FILE_4_EMPTY_STREAM);
        StorageFile storageFile = new StorageFile()
                .storage(cacheFactoryInfo.getStorage())
                .bucketName(BUCKET)
                .fileName(PARQUET_FILE_4_EMPTY_STREAM);
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageFile);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) inputStream.getFileSize());
        assertNotNull(byteBuffer, "Byte buffer is null");
        inputStream.readFully(byteBuffer);
    }

    @Test
    public void testEmptyGCSInputFile() throws IOException, URISyntaxException
    {
        FileCacheFactoryInfoTest cacheFactoryInfo = prepareFileCacheFactoryForStorage(PARQUET_FILE_4_STREAM);
        GcsInputFile inputFile = FileCacheFactory.getEmptyGCSInputFile(cacheFactoryInfo.getStorage(), BUCKET, PARQUET_FILE_4_STREAM);
        assertNotNull(inputFile, "GCS input file was null");
    }

    @Test
    public void testReadBytesFromPosition() throws Exception
    {
        StorageWithInputTest storageWithInputTest = mockStorageWithInputFile(BUCKET, CSV_FILE);
        Storage st = storageWithInputTest.getStorage();
        ReadChannel ch = mock(ReadChannel.class);
        PowerMockito.when(st.reader(Mockito.any())).thenReturn(ch);
        PowerMockito.when(ch.read(Mockito.any())).thenReturn(1);
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageWithInputTest.getFileCache());
        byte[] bytes = "Hello".getBytes();
        inputStream.seek(4L);
        inputStream.read(bytes, 0, bytes.length);
        int zeroCount = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != 0) {
                zeroCount++;
            }
        }
        assertTrue("Read empty bytes", zeroCount != bytes.length);
    }

    @Test
    public void testReadBytes() throws Exception
    {
        StorageWithInputTest storageWithInputTest = mockStorageWithInputFile(BUCKET, PARQUET_FILE);
        Storage st = storageWithInputTest.getStorage();
        ReadChannel ch = mock(ReadChannel.class);
        PowerMockito.when(st.reader(Mockito.any())).thenReturn(ch);
        PowerMockito.when(ch.read(Mockito.any())).thenReturn(1);
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageWithInputTest.getFileCache());
        byte[] bytes = "Hello".getBytes();
        inputStream.seek(4L);
        inputStream.read(bytes);
        int zeroCount = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != 0) {
                zeroCount++;
            }
        }
        assertTrue("Read empty bytes", zeroCount != bytes.length);
    }

    @Test
    public void testReadFullyBytes() throws Exception
    {
        StorageWithInputTest storageWithInputTest = mockStorageWithInputFile(BUCKET, PARQUET_FILE);
        Storage st = storageWithInputTest.getStorage();
        ReadChannel ch = mock(ReadChannel.class);
        PowerMockito.when(st.reader(Mockito.any())).thenReturn(ch);
        PowerMockito.when(ch.read(Mockito.any())).thenReturn(1);
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageWithInputTest.getFileCache());
        byte[] bytes = "Hello".getBytes();
        inputStream.seek(4L);
        inputStream.readFully(bytes, 0, bytes.length);
        int zeroCount = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != 0) {
                zeroCount++;
            }
        }
        assertTrue("Read empty bytes", zeroCount != bytes.length);
    }

    @Test
    public void testReadByteBuffer() throws Exception
    {
        StorageWithInputTest storageWithInputTest = mockStorageWithInputFile(BUCKET, PARQUET_FILE);
        Storage st = storageWithInputTest.getStorage();
        PowerMockito.when(st.reader(Mockito.any())).thenReturn(mock(ReadChannel.class));
        SeekableGcsInputStream inputStream = new SeekableGcsInputStream(storageWithInputTest.getFileCache());
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) inputStream.getFileSize());
        long readLength = inputStream.read(byteBuffer);
        assertTrue("Read empty bytes", readLength != -1);
    }

    public void setUp() throws URISyntaxException, FileNotFoundException
    {
        st = mock(Storage.class);
        PowerMockito.when(st.get((BlobId) Mockito.any())).thenReturn(mock(Blob.class));
        URL resource = SeekableGcsInputStreamTest.class.getClassLoader().getResource(CSV_FILE);
        assert resource != null;
        cache = new StorageFile()
                .bucketName("world-customer")
                .fileName(CSV_FILE)
                .storage(st);

        seekableGCSInputStream = new SeekableGcsInputStream(cache);
    }

    @Test(expected = EOFException.class)
    public void testReadFully() throws Exception
    {
        setUp();
        byte[] s = "test".getBytes(StandardCharsets.UTF_8);
        seekableGCSInputStream.readFully(s);
    }

    @Test(expected = Exception.class)
    public void testReadFullyBuffer() throws IOException, URISyntaxException
    {
        setUp();
        byte[] s = "test123456".getBytes(StandardCharsets.UTF_8);
        seekableGCSInputStream.readFully(ByteBuffer.wrap(s));
    }

    @Test
    public void testRead() throws IOException, URISyntaxException
    {
        setUp();
        Assert.assertNotNull(seekableGCSInputStream.read());
    }

    @Test
    public void testReadDirectBuffer() throws IOException, URISyntaxException
    {
        setUp();
        InputStream inputStream = mock(InputStream.class);
        byte[] s = "test123456".getBytes(StandardCharsets.UTF_8);
        SeekableGcsInputStream.readDirectBuffer(inputStream, ByteBuffer.wrap(s), s);
    }
}
