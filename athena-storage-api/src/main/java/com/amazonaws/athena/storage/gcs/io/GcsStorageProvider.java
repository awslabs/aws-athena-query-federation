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

import com.amazonaws.athena.storage.common.PagedObject;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.gcs.GcsInputStream;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.athena.storage.common.PartitionUtil.isPartitionFolder;
import static com.amazonaws.athena.storage.gcs.io.FileCacheFactory.cacheBytesInTempFile;
import static com.amazonaws.athena.storage.gcs.io.FileCacheFactory.fromExistingCache;
import static java.util.Objects.requireNonNull;

public class GcsStorageProvider implements StorageProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorageProvider.class);

    private static final String MY_COE_NAME = "gcs";
    private final Storage storage;

    public GcsStorageProvider(String credentialJsonString) throws IOException
    {
        GoogleCredentials credentials
                = GoogleCredentials.fromStream(new ByteArrayInputStream(credentialJsonString.getBytes(StandardCharsets.UTF_8)))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    public static boolean accept(String providerCodeName)
    {
        return MY_COE_NAME.equalsIgnoreCase(providerCodeName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getOnlineInputStream(String bucket, String objectName)
    {
        GcsOnlineStream onlineStream = new GcsOnlineStream()
                .storage(storage)
                .bucketName(bucket)
                .fileName(objectName);
        return new GcsInputStream(onlineStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream getOfflineInputStream(String bucket, String objectName) throws IOException
    {
        requireNonNull(storage, "Storage was null");
        File tempFile = fromExistingCache(bucket, objectName);
        if (tempFile == null) {
            LOGGER.debug("StorageProvider=GcsStorageProvider|Method=getOfflineInputStream|Message=File {} under the bucket {} not cached. Caching...",
                    objectName, bucket);
            GcsFileByteLoader byteLoader = new GcsFileByteLoader(storage, bucket, objectName);
            tempFile = cacheBytesInTempFile(bucket, objectName, byteLoader.getData());
        }
        LOGGER.debug("StorageProvider=GcsStorageProvider|Method=getOfflineInputStream|Message=Returning cached file {} under the bucket {}",
                objectName, bucket);
        GcsOfflineStream offlineStream = new GcsOfflineStream()
                .bucketName(bucket)
                .fileName(objectName)
                .file(tempFile)
                .storage(storage);
        return new GcsInputStream(offlineStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SeekableInputStream getSeekableInputStream(String bucket, String objectName)
    {
        return null;
    }

    @Override
    public InputFile getInputFile(String bucket, String objectName) throws IOException
    {
        StorageFile storageFile = createRandomFile(storage, bucket, objectName);
        return new GcsInputFile(storageFile);
    }

    @Override
    public List<String> getAllBuckets()
    {
        List<String> bucketNames = new ArrayList<>();
        Page<Bucket> buckets = storage.list();
        for (Bucket bucket : buckets.iterateAll()) {
            bucketNames.add(bucket.getName());
        }
        return ImmutableList.copyOf(bucketNames);
    }

    @SuppressWarnings("unused")
    @Override
    public boolean isDirectory(String bucket, String prefix)
    {
        BlobId blobId = BlobId.of(bucket, prefix);
        Blob blob = storage.get(blobId);
        if (blob == null && !prefix.endsWith("/")) { // maybe a folder without ending with a '/' character
            blob = storage.get(BlobId.of(bucket, prefix + "/"));
        }
        LOGGER.debug("Blob for prefix {} under the bucket {} is: {} with size: {}", prefix, bucket, blob, blob == null ? -1 : blob.getSize());
        return  (blob != null && blob.getSize() == 0);
    }

    @Override
    public boolean isPartitionedDirectory(String bucket, String location)
    {
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.prefix(location));
        for (Blob blob : blobPage.iterateAll()) {
            if (isPartitionFolder(blob.getName())) {
                LOGGER.info("Path {} is a partitioned directory", location);
                return true;
            }
        }
        LOGGER.info("Path {} is NOT a partitioned directory", location);
        return false;
    }

    @Override
    public List<String> getObjectNames(String bucket)
    {
        return toImmutableObjectNameList(storage.list(bucket, Storage.BlobListOption.currentDirectory()));
    }

    @Override
    public List<String> getNestedFolders(String bucket, String prefix)
    {
        if (!prefix.endsWith("/")) {
            prefix += '/';
        }
        List<String> folderNames = new ArrayList<>();
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            if (blob.getSize() == 0) { // it's a folder
                folderNames.add(blob.getName());
            }
        }
        return ImmutableList.copyOf(folderNames);
    }

    @Override
    public PagedObject getObjectNames(String bucket, String continuationToken, int pageSize)
    {
        Storage.BlobListOption maxTableCountOption = Storage.BlobListOption.pageSize(pageSize);
        Page<Blob> blobs;
        if (continuationToken != null) {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                    Storage.BlobListOption.pageToken(continuationToken), maxTableCountOption);
        }
        else {
            blobs = storage.list(bucket, Storage.BlobListOption.currentDirectory(), maxTableCountOption);
        }
        return PagedObject.builder()
                .fileNames(toImmutableObjectNameList(blobs))
                .nextToken(blobs.getNextPageToken())
                .build();
    }

    @Override
    public long getFileSize(String bucket, String file)
    {
        BlobId blobId = BlobId.of(bucket, file);
        return storage.get(blobId).getSize();
    }

    @Override
    public Optional<String> getFirstObjectNameRecurse(String bucket, String prefix)
    {
        if (!prefix.endsWith("/")) {
            prefix += '/';
        }
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            LOGGER.debug("GcsStorageProvider.getFirstObjectNameRecurse(): checking if {} is a folder under prefix {}", blob.getName(), prefix);
            if (prefix.equals(blob.getName())) {
                continue;
            }
            if (blob.getSize() > 0) { // it's a file
                return Optional.of(blob.getName());
            }
            else {
                return getFirstObjectNameRecurse(bucket, blob.getName());
            }
        }
        return Optional.empty();
    }

    @Override
    public List<String> getLeafObjectsByPartitionPrefix(String bucket, String partitionPrefix, int maxCount)
    {
        LOGGER.debug("Iterating recursively through a folder under the bucket to list all file object");
        List<String> leaves = new ArrayList<>();
        getLeafObjectsRecurse(bucket, partitionPrefix, leaves, maxCount);
        return leaves;
    }

    // helpers
    private void getLeafObjectsRecurse(String bucket, String prefix, List<String> leafObjects, int maxCount)
    {
        if (maxCount > 0 && leafObjects.size() >= maxCount) {
            return;
        }
        LOGGER.debug("Walking through {} under bucket '{}'", prefix, bucket);
        if (!prefix.endsWith("/")) {
            prefix += '/';
        }
        Page<Blob> blobPage = storage.list(bucket, Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(prefix));
        for (Blob blob : blobPage.iterateAll()) {
            if (blob.getName().equals(prefix)) {
                continue;
            }
            if (blob.getSize() > 0) { // it's a file
                leafObjects.add(blob.getName());
            }
            else {
                getLeafObjectsRecurse(bucket, blob.getName(), leafObjects, maxCount);
            }
        }
    }

    private StorageFile createRandomFile(Storage storage,
                                         String bucketName,
                                         String fileName)
    {
        return new StorageFile()
                .storage(storage)
                .bucketName(bucketName)
                .fileName(fileName);
    }

    private List<String> toImmutableObjectNameList(Page<Blob> blobs)
    {
        List<String> blobNameList = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob != null) {
                blobNameList.add(blob.getName());
            }
        }
        LOGGER.debug("blobNameList\n{}", blobNameList);
        return ImmutableList.copyOf(blobNameList);
    }

    private List<String> toImmutableFolderNameList(Page<Blob> blobs)
    {
        List<String> blobNameList = new ArrayList<>();
        for (Blob blob : blobs.iterateAll()) {
            if (blob != null
                    && blob.getSize() == 0) {
                blobNameList.add(blob.getName());
            }
        }
        LOGGER.debug("blobNameList\n{}", blobNameList);
        return ImmutableList.copyOf(blobNameList);
    }

    private boolean isRootFolder(String location)
    {
        return (location != null && !location.isBlank() && location.split("/").length == 1);
    }
}
