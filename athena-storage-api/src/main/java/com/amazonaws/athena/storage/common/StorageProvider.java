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
package com.amazonaws.athena.storage.common;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface StorageProvider
{
    /**
     * Provides an online stream that will read byte(s) directly from storage stream
     *
     * @param bucket Name of the bucket
     * @param objectName Name of the object (at the most cases, this is a file, maybe without extension)
     * @return An instance of storage type specific implementation of {@link InputStream}
     */
    InputStream getOnlineInputStream(String bucket, String objectName);

    /**
     * Provides an offline stream that will read byte(s) directly from storage stream
     * In some cases, it's not feasible to read byte by byte (i.e., single byte) read/parse data (metadata/records traversal).
     * This is because reading byte-by-byte will dramatically slow down the overall performance
     * For example, CSV parse reads records byte by byte.
     * An offline storage stream caches the entire bytes from the storage object into the Lambda's ephemeral storage
     *
     * @param bucket Name of the bucket
     * @param objectName Name of the object (at the most cases, this is a file, maybe without extension)
     * @return An instance of storage type specific implementation of {@link InputStream}
     */
    InputStream getOfflineInputStream(String bucket, String objectName) throws IOException;

    /**
     * Provides an online stream that will read byte(s) directly from storage stream
     *
     * @param bucket Name of the bucket
     * @param objectName Name of the object (at the most cases, this is a file, maybe without extension)
     * @return An instance of storage type specific implementation of {@link SeekableInputStream}
     */
    SeekableInputStream getSeekableInputStream(String bucket, String objectName);

    /**
     * Provides an instance of {@link InputFile} to read parquet file
     *
     * @param bucket Name of the bucket
     * @param objectName Name of the object (at the most cases, this is a file, maybe without extension)
     * @return An instance of storage type specific implementation of {@link InputFile}
     */
    InputFile getInputFile(String bucket, String objectName) throws IOException;

    /**
     * List name of the all buckets
     * @return List of bucket names
     */
    List<String> getAllBuckets();

    /**
     * Inicates whether a location (aka, prefix) under a bucket is a directory
     * @param bucket Name of the bucket
     * @param location The path of the object, sometimes called prefix in some popular storage provider
     * @return true if the path is a directory (folder), false otherwise
     */
    boolean isDirectory(String bucket, String location);

    /**
     * Retrieves a list of file names
     * @param bucket Name of the bucket from where the underlying provider retrieves the files
     * @return List of all file names
     */
    List<String> getFileNames(String bucket);

    PagedObject getFileNames(String bucket, String continuationToken, int pageSize);

    /**
     * Determine the file size
     * @param bucket Name of the bucket
     * @param file Name of the file
     * @return Size of the file under the specified bucket
     */
    long getFileSize(String bucket, String file);

    Optional<String> getFirstObjectNameRecurse(String bucket, String prefix);
}
