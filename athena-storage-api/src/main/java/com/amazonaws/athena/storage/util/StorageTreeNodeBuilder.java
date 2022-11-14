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
package com.amazonaws.athena.storage.util;

import com.amazonaws.athena.storage.common.StorageNode;
import com.amazonaws.athena.storage.common.StorageProvider;
import com.amazonaws.athena.storage.common.TreeTraversalContext;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.amazonaws.athena.storage.io.StorageIOUtil.getParentPath;

public class StorageTreeNodeBuilder
{
    private StorageTreeNodeBuilder()
    {
    }

    public static Optional<StorageNode<String>> buildTableTree(String bucket)
    {
        return Optional.empty();
    }

    public static synchronized Optional<StorageNode<String>> buildTreeWithPartitionedDirectories(String bucket,
                                                                                                 String rootName,
                                                                                                 String rootPath,
                                                                                                 TreeTraversalContext context)
    {
        if (rootPath.endsWith("/")) {
            rootPath = rootPath.replace("/", "");
        }
        StorageProvider storageProvider = context.getStorageDatasource().getStorageProvider();
        List<String> paths = storageProvider.getNestedFolders(bucket, rootPath);
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
                if (!context.isIncludeFile() && context.isFile(bucket, path)) {
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
                    parent = parent.addChild(names[i], path);
                }
                else {
                    parent = parent.addChild(names[i], path);
                }
            }
        }
        return Optional.of(root);
    }

    public static synchronized Optional<StorageNode<String>> buildFileOnlyTreeForPrefix(String bucket,
                                                                                        String rootName,
                                                                                        String prefix,
                                                                                        TreeTraversalContext context)
    {
        StorageProvider storageProvider = context.getStorageDatasource().getStorageProvider();
        List<String> paths = storageProvider.getLeafObjectsByPartitionPrefix(bucket, prefix, Integer.MAX_VALUE);
        if (paths.isEmpty()) {
            return Optional.empty();
        }
        StorageNode<String> root = new StorageNode<>(rootName, rootName);
        for (String data : paths) {
            String[] names = context.normalizePaths(data.split("/"));
            if (names.length == 0) {
                continue;
            }
            StorageNode<String> parent = root;
            for (int i = 0; i < names.length; i++) {
                if (parent.getPath().equals(names[i])) {
                    continue;
                }
                String path = String.join("/",
                        Arrays.copyOfRange(names, 0, i + 1));
                if (!context.isFile(bucket, path)) {
                    continue;
                }
                Optional<StorageNode<String>> optionalParent = root.findByPath(getParentPath(path));
                if (optionalParent.isPresent()) {
                    parent = optionalParent.get();
                    if (parent.getPath().equals(path)) {
                        continue;
                    }
                    parent = parent.addChild(names[i], path);
                }
                else {
                    parent = parent.addChild(names[i], path);
                }
            }
        }
        return Optional.of(root);
    }
}
