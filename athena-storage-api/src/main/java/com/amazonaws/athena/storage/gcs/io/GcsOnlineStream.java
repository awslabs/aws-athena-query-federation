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

import com.amazonaws.athena.storage.gcs.SeekableGcsInputStream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

/**
 * Represents a cached file and conveniently provides underlying object instances and other information related
 * to cached file to the other classes, especially to {@link SeekableGcsInputStream}
 * <p>
 * Setters in this class are fluent-styled
 */
public class GcsOnlineStream
{
    private Storage storage;
    private String bucketName;
    private String fileName;
    private long length;
    private boolean initialized;

    /**
     * Sets storage
     *
     * @param storage An instance of {@link Storage}
     * @return An instance of {@link GcsOfflineStream}
     */
    public GcsOnlineStream storage(Storage storage)
    {
        this.storage = storage;
        return this;
    }

    /**
     * @return The storage object
     */
    public Storage storage()
    {
        return storage;
    }

    /**
     * @return The name of the bucket
     */
    public String bucketName()
    {
        return bucketName;
    }

    /**
     * Sets the bucket name
     *
     * @param bucketName Name of the bucket to set
     * @return Self
     */
    public GcsOnlineStream bucketName(String bucketName)
    {
        this.bucketName = bucketName;
        return this;
    }

    /**
     * @return Name of the file
     */
    public String fileName()
    {
        return fileName;
    }

    /**
     * Sets the file name
     *
     * @param fileName Name of the file
     * @return Self
     */
    public GcsOnlineStream fileName(String fileName)
    {
        this.fileName = fileName;
        return this;
    }

    /**
     * @return Length of the remote file object
     */
    public long getLength()
    {
        if (initialized) {
            return length;
        }
        BlobId blobId = BlobId.of(bucketName, fileName);
        Blob blob = storage.get(blobId);
        length = blob.getSize();
        initialized = true;
        return length;
    }
}
