/*-
 * #%L
 * athena-storage-api
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.amazonaws.athena.storage.gcs;

import com.amazonaws.athena.storage.gcs.io.GcsOfflineStream;
import com.amazonaws.athena.storage.gcs.io.GcsOnlineStream;
import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class GcsInputStream extends InputStream
{
    private static final Logger logger = LoggerFactory.getLogger(GcsInputStream.class);
    private long currentPosition = 0;
    private final Storage storage;
    private final BlobId blobId;
    private ReadChannel readChannel;
    private final long fileSize;
    private InputStream inputStream;

    /**
     * Constructor to instantiate this input stream instance for the given instance {@link StorageFile}
     *
     * @param gcsOnlineStream An instance of {@link GcsOnlineStream}
     */
    public GcsInputStream(GcsOnlineStream gcsOnlineStream)
    {
        this.storage = gcsOnlineStream.storage();
        this.blobId = BlobId.of(gcsOnlineStream.bucketName(), gcsOnlineStream.fileName());
        Blob blob = storage.get(blobId);
        this.fileSize = blob.getSize();
        openReadChannel();
    }

    /**
     * Constructor to instantiate this input stream instance using the given instance of {@link GcsOfflineStream}
     *
     * @param gcsOfflineStream An instance of {@link GcsInputStream}
     */
    public GcsInputStream(GcsOfflineStream gcsOfflineStream)
    {
        this.storage = gcsOfflineStream.storage();
        this.blobId = BlobId.of(gcsOfflineStream.bucketName(), gcsOfflineStream.fileName());
        Blob blob = storage.get(blobId);
        this.fileSize = blob.getSize();
        this.inputStream = gcsOfflineStream.inputStream();
    }

    /**
     * Opens up a read channel to start reading from GCS storage object
     */
    private void openReadChannel()
    {
        this.readChannel = storage.reader(blobId);
    }

    /**
     * @return Size of teh file
     */
    public long getFileSize()
    {
        return fileSize;
    }

    /**
     * {{@inheritDoc}}
     *
     * @return
     * @throws IOException
     */
    @Override
    public int read() throws IOException
    {
        long toBytePosition = this.currentPosition + 1;
        if (toBytePosition >= this.fileSize) {
            return -1;
        }
        int readData;
        if (inputStream != null) {
            readData = readCache();
            if (readData != -1) {
                this.currentPosition += 1;
            }
            return readData;
        }
        byte[] bytes = new byte[1];
        try (ReadChannel readChannel = storage.reader(blobId)) {
            readChannel.seek(this.currentPosition);
            readChannel.limit(1);
            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            int readLength = 0;
            int len;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            readChannel.read(byteBuffer);
            len = byteBuffer.array().length;
            while (len > 0) {
                outputStream.write(byteBuffer.array(), 0, len);
                byteBuffer.clear();
                readLength += byteBuffer.array().length;
                if (len >= bytes.length) {
                    break;
                }
                readChannel.read(byteBuffer);
                len = byteBuffer.array().length;
            }
            byte[] source = outputStream.toByteArray();
            if (source.length == 0) {
                return -1;
            }
            System.arraycopy(source, 0, bytes, 0, bytes.length);
            readData = bytes[0] & 0xff;
        }
        this.currentPosition = toBytePosition;
        return readData;
    }

    /**
     * {{@inheritDoc}}
     *
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public int read(@Nonnull byte[] bytes) throws IOException
    {
        return read(bytes, 0, bytes.length);
    }

    /**
     * {{@inheritDoc}}
     *
     * @param bytes
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    @Override
    public int read(@Nonnull byte[] bytes, int offset, int length) throws IOException
    {
        if (this.currentPosition >= fileSize) {
            return -1;
        }
        if (length >= this.fileSize - 1) {
            length = (int) (length - fileSize);
            length = (int) min(this.fileSize, length);
        }
        if (inputStream != null) {
            int bytesRead = readCachedBytes(bytes);
            if (bytesRead > 0) {
                this.currentPosition += bytesRead;
                return bytesRead;
            }
            else {
                return -1;
            }
        }

        try (ReadChannel readChannel = storage.reader(blobId)) {
            readChannel.seek(this.currentPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(length);

            int len = 0;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            while ((len += readChannel.read(byteBuffer)) < length) {
                // do nothing
            }
            if (len < 0) {
                throw new EOFException();
            }
            byte[] byteArray = byteBuffer.array();

            len = toIntExact(min(byteArray.length, len));
            outputStream.write(byteArray, 0, len);
            byte[] source = outputStream.toByteArray();
            System.arraycopy(source, 0, bytes, 0, len);
            this.currentPosition += len;
            return len;
        }
    }

    /**
     * {{@inheritDoc}}
     *
     * @param bytes
     * @param offset
     * @param length
     * @return
     */
    @Override
    public int readNBytes(byte[] bytes, int offset, int length)
    {
        return -1;
    }

    /**
     * {{@inheritDoc}}
     */
    @Override
    public void close()
    {
        if (this.readChannel != null) {
            this.readChannel.close();
        }
    }

    // helpers

    /**
     * Reads bytes from the cached file
     *
     * @param bytes Bytes array to read in
     * @return Number of bytes read
     * @throws IOException If occurs any during the read operation
     */
    private int readCachedBytes(byte[] bytes) throws IOException
    {
        if (inputStream == null) {
            return -1;
        }
        int readLength = 0;
        for (int i = 0; i < bytes.length; i++) {
            int read = inputStream.read();
            if (read == -1) {
                break;
            }
            bytes[i] = (byte) read;
            readLength++;
        }
        return readLength;
    }

    /**
     * Reads a byte from the cached file
     *
     * @return Byte read
     * @throws IOException If occurs any during the read operation
     */
    private int readCache() throws IOException
    {
        if (this.inputStream != null) {
            return inputStream.read();
        }
        return -1;
    }
}
