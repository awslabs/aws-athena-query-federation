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
package com.amazonaws.athena.storage.gcs.io;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class GcsFileByteLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsFileByteLoader.class);

    protected final Storage storage;
    protected final String bucket;
    protected final String filename;
    protected final long length;
    protected final BlobId blobId;
    protected final long startOffset;
    protected final long endOffset;

    protected byte[] data;

    /**
     * A byte loader from file resides in Google Cloud Storage. It reads the entire file bytes into a local bytes
     * array so that it can be retrieved quickly. Otherwise, it needs to do many round trips to read the data bytes
     * directly from GCS
     *
     * @param storage    An instance of {@link Storage}
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     */
    public GcsFileByteLoader(Storage storage, String bucketName, String fileName) throws IOException
    {
        this.storage = storage;
        this.bucket = bucketName;
        this.filename = fileName;
        this.startOffset = 0;
        this.blobId = BlobId.of(bucketName, fileName);
        Blob blob = storage.get(blobId);
        this.length = blob.getSize();
        LOGGER.info("File size to download in cache is {} from file {} under the bucket {}", this.length, fileName, bucketName);
        this.endOffset = this.length - 1;
        loadInternalData();
    }

    /**
     * Reads data bytes from GCS into the local byte array
     *
     * @throws IOException If occurs during read
     */
    protected void loadInternalData() throws IOException
    {
        try (ReadChannel readChannel = storage.reader(blobId)) {
            readChannel.seek(startOffset);
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) length);
            long readLength = this.length;
            int len = readChannel.read(byteBuffer);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            while (len > 0 && readLength > 0) {
                outputStream.write(byteBuffer.array(), 0, (int) Math.min(len, readLength));
                byteBuffer.clear();
                readLength -= len;
                len = readChannel.read(byteBuffer);
            }
            byte[] source = outputStream.toByteArray();
            data = new byte[(int) this.length];
            System.arraycopy(source, 0, data, 0, (int) length);
        }
    }

    /**
     * @return Internal cached bytes
     */
    protected byte[] getData()
    {
        return this.data;
    }

    /**
     * @return Underlying instance of {@link Storage}
     */
    public Storage getStorage()
    {
        return storage;
    }

    /**
     * @return Underlying bucket name
     */
    public String getBucket()
    {
        return bucket;
    }

    /**
     * @return Underlying file name
     */
    public String getFilename()
    {
        return filename;
    }

    /**
     * @return Length of the file
     */
    public long getLength()
    {
        return length;
    }
}
