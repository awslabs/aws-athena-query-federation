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

import com.amazonaws.athena.storage.gcs.io.StorageFile;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.apache.parquet.io.SeekableInputStream;
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

public class SeekableGcsInputStream extends SeekableInputStream
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SeekableGcsInputStream.class);

    private final byte[] temp = new byte[8192];
    private long currentPosition = 0;
    private final Storage storage;
    private final BlobId blobId;
    private final long fileSize;

    /**
     * Constructor to instantiate this input stream instance for the given instance {@link StorageFile}
     *
     * @param storageFile An instance of {@link SeekableGcsInputStream}
     */
    public SeekableGcsInputStream(StorageFile storageFile)
    {
        this.storage = storageFile.storage();
        LOGGER.debug("Creating blob id for file {} under teh bucket {}", storageFile.fileName(), storageFile.bucketName());
        this.blobId = BlobId.of(storageFile.bucketName(), storageFile.fileName());
        Blob blob = storage.get(blobId);
        this.fileSize = blob.getSize();
    }

    /**
     * @param storageFile fileCache An instance of {@link SeekableGcsInputStream}
     * @param fileLength  Length of  the file
     */
    public SeekableGcsInputStream(StorageFile storageFile, long fileLength)
    {
        this.storage = storageFile.storage();
        this.blobId = BlobId.of(storageFile.bucketName(), storageFile.fileName());
        this.fileSize = fileLength;
    }

    public long getFileSize()
    {
        return fileSize;
    }

    @Override
    public long getPos()
    {
        return currentPosition;
    }

    /**
     * Place the file read pointer to a specific position in the input stream
     *
     * @param position Position to set
     */
    @Override
    public void seek(long position)
    {
        this.currentPosition = position;
    }

    @Override
    public void readFully(byte[] bytes) throws IOException
    {
        readFully(bytes, (int) this.currentPosition, bytes.length);
    }

    /**
     * Reads number of bytes form the given position and length into the bytes array. It first tries to see if there is an
     * underlying file cache, if found, it reads from the cached file. Otherwise, it reads directly from the GCS read channel
     *
     * @param b        Bytes array to read in
     * @param position Read position
     * @param length   Length of the bytes to read
     * @throws IOException If occurs during read operation
     */
    @Override
    public void readFully(byte[] b, int position, int length) throws IOException
    {
        int n = 0;
        do {
            int count = this.read(b, position + n, length - n);
            if (count < 0) {
                throw new EOFException();
            }
            n += count;
        } while (n < length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(ByteBuffer buffer) throws IOException
    {
        return buffer.hasArray() ? readHeapBuffer(this, buffer) : readDirectBuffer(this, buffer, this.temp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFully(ByteBuffer buffer) throws IOException
    {
        if (buffer.hasArray()) {
            readFullyHeapBuffer(this, buffer);
        }
        else {
            readFullyDirectBuffer(this, buffer, this.temp);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException
    {
        if (this.currentPosition >= fileSize) {
            return -1;
        }
        int readData;
        byte[] bytes = new byte[1];
        int len;
        try (ReadChannel readChannel = storage.reader(blobId)) {
            readChannel.seek(this.currentPosition);
            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            len = readChannel.read(byteBuffer);
            if (len < 0) {
                return -1;
            }
            outputStream.write(byteBuffer.array());
            byte[] source = outputStream.toByteArray();
            if (source.length == 0) {
                return -1;
            }
            System.arraycopy(source, 0, bytes, 0, bytes.length);
            readData = bytes[0] & 0xff;
        }
        this.currentPosition += len;
        return readData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(@Nonnull byte[] bytes) throws IOException
    {
        return read(bytes, 0, bytes.length);
    }

    /**
     * {@inheritDoc}
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
     * {@inheritDoc}
     */
    public int readNBytes(byte[] bytes, int offset, int length)
    {
        return -1;
    }

    public void close()
    {
    }

    // static helper
    /**
     * Mimics reading bytes from heap buffer. Basically it accesses the bytes directly from GCS storage object
     *
     * @param stream An instance of {@link SeekableGcsInputStream}
     * @param buffer Buffer into which bytes will be read
     * @return Total bytes read
     * @throws IOException Occurs if any during read or any IO operations
     */
    static int readHeapBuffer(InputStream stream, ByteBuffer buffer) throws IOException
    {
        int bytesRead = stream.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        if (bytesRead >= 0) {
            buffer.position(buffer.position() + bytesRead);
        }
        return bytesRead;
    }

    /**
     * Reads byes directly from GCS storage object
     *
     * @param stream An instance of {@link SeekableGcsInputStream}
     * @param buffer Buffer into which bytes will be read
     * @param temp   Temporary buffer
     * @return Total bytes read if any, -1 otherwise
     * @throws IOException if occurs any during read or any IO operations
     */
    static int readDirectBuffer(InputStream stream, ByteBuffer buffer, byte[] temp) throws IOException
    {
        // copy all the bytes that return immediately, stopping at the first
        // read that doesn't return a full buffer.
        int nextReadLength = min(buffer.remaining(), temp.length);

        int totalBytesRead;
        int bytesRead = stream.read(temp, 0, nextReadLength);
        for (totalBytesRead = 0; bytesRead == temp.length; ) {
            buffer.put(temp);
            totalBytesRead += bytesRead;
            bytesRead = stream.read(temp, 0, nextReadLength);
            nextReadLength = min(buffer.remaining(), temp.length);
        }

        if (bytesRead < 0) {
            return totalBytesRead == 0 ? -1 : totalBytesRead;
        }
        else {
            buffer.put(temp, 0, bytesRead);
            totalBytesRead += bytesRead;
            return totalBytesRead;
        }
    }

    /**
     * Mimics reading from heap buffer though the bytes are accessed directly from the GCS storage object
     *
     * @param stream An instance of {@link SeekableGcsInputStream}
     * @param buffer
     * @throws IOException
     */
    static void readFullyHeapBuffer(InputStream stream, ByteBuffer buffer) throws IOException
    {
        readFully(stream, buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        buffer.position(buffer.limit());
    }

    /**
     * Rads full bytes into the provided buffer
     *
     * @param stream An instance of {@link SeekableGcsInputStream}
     * @param bytes  byte array into which bytes will copied to
     * @param start  start position in the byte array
     * @param len    Total length to read
     * @throws IOException If occurs any during read of any IO operations
     */
    static void readFully(InputStream stream, byte[] bytes, int start, int len) throws IOException
    {
        int offset = start;
        int bytesRead;
        for (int remaining = len; remaining > 0; offset += bytesRead) {
            bytesRead = stream.read(bytes, offset, remaining);
            if (bytesRead < 0) {
                throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read");
            }
            remaining -= bytesRead;
        }
    }

    /**
     * Reads byes directly from GCS storage object
     *
     * @param stream An instance of {@link SeekableGcsInputStream}
     * @param buffer Buffer into which the bytes will be read
     * @param temp   Temporary bytes
     * @throws IOException
     */
    static void readFullyDirectBuffer(InputStream stream, ByteBuffer buffer, byte[] temp) throws IOException
    {
        int nextReadLength = min(buffer.remaining(), temp.length);
        int bytesRead;
        for (bytesRead = 0; nextReadLength > 0 && bytesRead >= 0; ) {
            buffer.put(temp, 0, bytesRead);
            bytesRead = stream.read(temp, 0, nextReadLength);
            nextReadLength = min(buffer.remaining(), temp.length);
        }

        if (bytesRead < 0 && buffer.remaining() > 0) {
            throw new EOFException("Reached the end of stream with " + buffer.remaining() + " bytes left to read");
        }
    }
}
