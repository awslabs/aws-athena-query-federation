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
package com.amazonaws.athena.connectors.gcs.common;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.athena.connectors.gcs.common.StorageIOUtil.getParentPath;
import static com.amazonaws.athena.connectors.gcs.storage.AbstractStorageDatasource.getLeafObjectsByPartitionPrefix;
import static com.amazonaws.athena.connectors.gcs.storage.AbstractStorageDatasource.isDirectory;

/**
 * A tree node builder builds a tree of child nodes much like a file tree seen in the popular GUI based OS. For example,
 * a file explorer in Windows.  From the root to children, all are the instance of {@link StorageNode}.
 * Currently it's generic, because a specific storage provider may have a different kind of object to represent a buckets/files/folders
 * Unlike other three structure, the node has storage specific information.
 *
 */
public class StorageTreeNodeBuilder
{
    private StorageTreeNodeBuilder()
    {
    }

    /**
     * When a folder is partitioned, that is, it contains one or more FIELD_NAME=FIELD_VALUE patterned sub-folder, this method reads all
     * nested partitioned folder(s)
     * @param bucket Name of the bucket
     * @param rootName Name of the root node
     * @param rootPath Path of the root node
     * @param context An instance of {@link TreeTraversalContext} that tells upto which level, it needs to traverse, what is the start level
     *                from where it should checking and loading partitioned folder(s), whether to load files or not and much more
     * @return An optional instance of {@link StorageNode} as the root node with one or more children
     */
    public static synchronized Optional<StorageNode<String>> buildTreeWithPartitionedDirectories(String bucket,
                                                                                                 String rootName,
                                                                                                 String rootPath,
                                                                                                 TreeTraversalContext context)
    {
        if (rootPath.endsWith("/")) {
            rootPath = rootPath.replace("/", "");
        }
        Storage storage = context.getStorage();
        List<String> paths = getNestedFolders(bucket, rootPath, storage);
        if (paths.isEmpty()) {
            return Optional.empty();
        }
        StorageNode<String> root = new StorageNode<>(rootName, rootPath);
        for (String data : paths) {
            String[] names = context.normalizePaths(data.split("/"));
            if (names.length == 0 || !root.isChild(names[0])) {
                continue;
            }
            StorageNode<String> parent = root;
            for (int i = 0; i < names.length; i++) {
                if (parent.getPath().equals(names[i])) {
                    continue;
                }
                String path = String.join("/",
                        Arrays.copyOfRange(names, 0, i + 1));
                if (!context.isIncludeFile() && !isDirectory(bucket, path)) {
                    continue;
                }
                if (context.getPartitionDepth() > -1 && !context.isPartitioned(i, names[i])) {
                    continue;
                }
                Optional<StorageNode<String>> optionalParent = root.findByPath(getParentPath(path));
                if (optionalParent.isPresent()) {
                    parent = optionalParent.get();
                    if (parent.getPath().equals(path)) {
                        continue;
                    }
                }
                parent = parent.addChild(names[i], path);
            }
        }
        return Optional.of(root);
    }

    public static List<String> getNestedFolders(String bucket, String prefix, Storage storage)
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

    /**
     * Loads all supported files as per file_extension environment variable recursively within the prefix (usually a partitoned folder)
     * @param bucket Name of the bucket
     * @param rootName Name of the root node
     * @param prefix From where in the bucket, this method will load files. It's usually a partitioned folder
     * @param context An instance of {@link TreeTraversalContext} that tells upto which level, it needs to traverse, what is the start level
     *                from where it should checking and loading partitioned folder(s), whether to load files or not and much more
     * @return An optional instance of {@link StorageNode} as the root node with one or more children
     */
    public static synchronized Optional<StorageNode<String>> buildFileOnlyTreeForPrefix(String bucket,
                                                                                        String rootName,
                                                                                        String prefix,
                                                                                        TreeTraversalContext context)
    {
        System.out.printf("Retrieving files for root %s with prefix %s in the bucket %s with context%n%s%n",
                rootName, prefix, bucket, context);
        if (context.hasParent()) {
            prefix = getPrefixWithoutRoot(rootName, prefix);
        }
        List<String> paths = getLeafObjectsByPartitionPrefix(bucket, prefix, Integer.MAX_VALUE);
        System.out.printf("Got path of size %s, all paths are %n%s%n", paths.size(), paths);
        if (paths.isEmpty()) {
            return Optional.empty();
        }
        StorageNode<String> root = new StorageNode<>(rootName, rootName);
        for (String data : paths) {
            String path = data;
            if (context.hasParent()) {
                path = getPathWithRoot(rootName, data);
            }
            root.addChild(data, path);
        }
        return Optional.of(root);
    }

    private static String getPathWithRoot(String rootName, String path)
    {
        if (!rootName.endsWith("/")) {
            rootName += "/";
        }
        if (!path.startsWith(rootName)) {
            return rootName + path;
        }
        return path;
    }

    private static String getPrefixWithoutRoot(String root, String prefix)
    {
        if (!root.endsWith("/")) {
            root += "/";
        }
        if (prefix.startsWith(root)) {
            return prefix.substring(root.length());
        }
        return prefix;
    }
}
