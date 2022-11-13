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
package com.amazonaws.athena.storage.gcs.io;

import com.google.cloud.storage.Storage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;

import static java.util.Objects.requireNonNull;

public class FileCacheFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCacheFactory.class);

    /**
     * Minutes to retain a cached file in the ephemeral storage. After elapsing the max retention time,
     * it'll be deleted when the next call to cached/get a file is invoked
     */
    private static final int MAX_TEMP_FILE_RETENTION_MINUTES = 16;

    private FileCacheFactory()
    {
    }

    /**
     * Creates an instance of {@link StorageFile} based on the provided arguments
     *
     * @param storage    An instance of {@link Storage} to communicate with GCS
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     * @return An instance of {@link StorageFile}
     */
    public static synchronized StorageFile createRandomFile(Storage storage,
                                                            String bucketName,
                                                            String fileName)
    {
        return new StorageFile()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
    }

    /**
     * Creates a stream wrapper to read bytes directly from the GCS storage object
     *
     * @param storage    An instance of {@link Storage} to communicate with GCS
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     * @return Am instance of {@link GcsOnlineStream}
     */
    @Deprecated
    public static synchronized GcsOnlineStream createOnlineGcsStream(Storage storage,
                                                                     String bucketName,
                                                                     String fileName)
    {
        return new GcsOnlineStream()
                .bucketName(bucketName)
                .fileName(fileName)
                .storage(storage);
    }

    /**
     * Creates a stream wrapper to read bytes offline from the cached file
     *
     * @param storage    An instance of {@link Storage} to communicate with GCS
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     * @return Am instance of {@link GcsOfflineStream}
     */
    @Deprecated
    public static synchronized GcsOfflineStream createOfflineGcsStream(final Storage storage,
                                                                       String bucketName,
                                                                       String fileName) throws IOException
    {
            requireNonNull(storage, "Storage was null");
            File tempFile = fromExistingCache(bucketName, fileName);
            if (tempFile == null) {
                LOGGER.debug("Factory=FileCacheFactory|Method=createRandomFile|Message=File {} under the bucket {} not cached. Caching...",
                        fileName, bucketName);
                GcsFileByteLoader byteLoader = new GcsFileByteLoader(storage, bucketName, fileName);
                tempFile = cacheBytesInTempFile(bucketName, fileName, byteLoader.getData());
            }
            LOGGER.debug("Factory=FileCacheFactory|Method=createInputStream|Message=Returning cached file {} under the bucket {}",
                    fileName, bucketName);
            return new GcsOfflineStream()
                    .bucketName(bucketName)
                    .fileName(fileName)
                    .file(tempFile)
                    .storage(storage);
    }

    /**
     * Factory method to create an instance of {@link org.apache.parquet.io.InputFile} for the given arguments
     *
     * @param storage    An instance of {@link Storage} to communicate with GCS
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     * @return An instance of {@link GcsInputFile}, a subclass of {@link org.apache.parquet.io.InputFile}
     */
    @Deprecated
    public static GcsInputFile getGCSInputFile(Storage storage, String bucketName, String fileName) throws IOException
    {
        StorageFile storageFile = createRandomFile(storage, bucketName, fileName);
        return new GcsInputFile(storageFile);
    }

    /**
     * Factory method to create an instance of an empty {@link GcsInputFile} with null {@link RandomAccessFile} to force
     * the underlying input stream to read the bytes directly from Google Cloud Storage
     *
     * @param storage    An instance of {@link Storage} to communicate with GCS
     * @param bucketName Name of the bucket from GCS
     * @param fileName   Name of the file under the bucket
     * @return An instance of {@link GcsInputFile}, a subclass of {@link org.apache.parquet.io.InputFile}.
     */
    public static GcsInputFile getEmptyGCSInputFile(Storage storage, String bucketName, String fileName) throws IOException
    {
        return new GcsInputFile(new StorageFile()
                .bucketName(bucketName)
                .fileName(fileName)
                .storage(storage));
    }

    /**
     * Searches the cached file for the combination of bucket and file name
     *
     * @param bucketName Name of the bucket in GCS
     * @param fileName   Name of the file under the bucket
     * @return An instance of {@link File} if found, null otherwise
     * @throws IOException If occurs any
     */
    public static File fromExistingCache(String bucketName, String fileName) throws IOException
    {
        File tempFile = getTempFile(bucketName, fileName);
        if (tempFile != null && tempFile.exists()) {
            deleteExpiredCachedFile(tempFile.getPath());
            return tempFile;
        }
        return null;
    }

    /**
     * @param bucketName Name of the buckets
     * @param fileName Name of the file
     * @return The temporary file that was cached earlier
     */
    private static File getTempFile(String bucketName, String fileName)
    {
        String tempFileName = String.format("%s_%s.cache", bucketName, fileName);
        File tempDir = FileUtils.getTempDirectory();
        String tempFilePath = tempDir.getPath() + Path.SEPARATOR + tempFileName;
        File tempFile = new File(tempFilePath);
        if (tempFile.exists()) {
            return tempFile;
        }
        return null;
    }

    private static File createTempFile(String bucketName, String fileName)
    {
        String tempFileName = String.format("%s_%s.cache", bucketName, fileName);
        File tempDir = FileUtils.getTempDirectory();
        String tempFilePath = tempDir.getAbsolutePath() + Path.SEPARATOR + tempFileName;
        return new File(tempFilePath);
    }

    public static File cacheBytesInTempFile(String bucketName, String fileName, byte[] bytes) throws IOException
    {
        LOGGER.debug("Factory=FileCacheFactory|Message=caching file {}, length {}", fileName, bytes != null ? bytes.length : 0);
        LOGGER.debug("Caching file {} under the bucket {}", fileName, bucketName);
        File tempFile = getTempFile(bucketName, fileName);
        String path = tempFile != null ? tempFile.getPath() : "";
        deleteExpiredCachedFile(path);
        if (tempFile != null && bytes != null) {
            FileUtils.writeByteArrayToFile(tempFile, bytes);
        }
        else if (bytes != null) {
            tempFile = createTempFile(bucketName, fileName);
            FileUtils.writeByteArrayToFile(tempFile, bytes);
        }
        LOGGER.debug("Time took to cache bytes for the file {} under the bucket {}", fileName, bucketName);
        return tempFile;
    }

    /**
     * Checks to see if a file other than the exceptFile, are expired. If an expired found, it'll be deleted
     *
     * @param exceptFile File to ignore from checking as it's being used
     */
    private static int deleteExpiredCachedFile(String exceptFile)
    {
        FileFilter fileFilter = new WildcardFileFilter(Arrays.asList("*.cache"));
        File tempDir = FileUtils.getTempDirectory();
        File[] cachedFiles = tempDir.listFiles(fileFilter);
        if (cachedFiles == null || cachedFiles.length == 0) {
            return 0;
        }
        int deleteCount = 0;
        for (File tempFile : cachedFiles) {
            if (tempFile.getPath().equals(exceptFile)) {
                continue;
            }
            BasicFileAttributes attributes = null;
            try {
                attributes = Files.readAttributes(tempFile.toPath(), BasicFileAttributes.class);
            }
            catch (Exception exception) {
                // Ignored
                LOGGER.error("Factory=FileCacheFactory|Message=Unable to read file attributes. Error={}",
                        exception.getMessage());
            }
            if (attributes != null) {
                FileTime creationTime = attributes.creationTime();
                Duration timeDifference = Duration.between(creationTime.toInstant(), new Date().toInstant());
                if (timeDifference.toMinutes() >= MAX_TEMP_FILE_RETENTION_MINUTES) {
                    if (tempFile.delete()) {
                        deleteCount++;
                    }
                }
            }
        }
        return deleteCount;
    }
}
