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
import com.google.cloud.storage.Bucket;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getUniqueEntityName;
import static com.amazonaws.athena.connectors.gcs.storage.StorageUtil.getValidEntityName;

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

    public static Optional<StorageNode<String>> buildSchemaList(TreeTraversalContext context, String breakPoint)
    {
        StorageNode<String> root = new StorageNode<>("", "");
        Page<Bucket> buckets = context.getStorage().list();
        Map<String, String> schemas = new HashMap<>();
        for (Bucket bucket : buckets.iterateAll()) {
            String bucketName = bucket.getName();
            String validName = getValidEntityName(bucketName);
            if (schemas.containsKey(validName)) {
                validName = getUniqueEntityName(validName, schemas);
            }
            schemas.put(validName, bucketName);
            if (validName.equals(breakPoint)) {
                break;
            }
        }
        schemas.forEach(root::addChild);
        return Optional.of(root);
    }
}
