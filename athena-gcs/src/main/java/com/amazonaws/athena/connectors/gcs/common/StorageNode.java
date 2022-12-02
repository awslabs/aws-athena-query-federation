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

import java.util.Optional;
import java.util.TreeSet;

public class StorageNode<T extends Comparable<T>> implements Comparable<StorageNode<T>>
{
    private final TreeSet<StorageNode<T>> children = new TreeSet<>();
    private StorageNode<T> parent = null;
    private final T data;
    private T path;
    private int index = 0;

    public StorageNode(T data, T path)
    {
        this.data = data;
        this.path = path;
    }

    public TreeSet<StorageNode<T>> getChildren()
    {
        return children;
    }

    public StorageNode<T> addChild(T data, T path)
    {
        StorageNode<T> child = new StorageNode<>(data, path);
        child.parent = this;
        child.index = this.children.size();
        this.children.add(child);
        return child;
    }

    public T getData()
    {
        return this.data;
    }

    public T getPath()
    {
        return path;
    }

    public boolean isRoot()
    {
        return (this.parent == null);
    }

    public boolean isLeaf()
    {
        return this.children.size() == 0;
    }

    public boolean isChild(String path)
    {
        return ((String) this.path).isBlank() || this.path.equals("/") || this.path.equals(path);
    }

    public Optional<StorageNode<T>> findChildByData(String data)
    {
        for (StorageNode<T> node : children) {
            if (node.data.equals(data)) {
                return Optional.of(node);
            }
        }
        return Optional.empty();
    }

    public Optional<StorageNode<T>> findByPath(String path)
    {
        if (this.path.equals(path)) {
            return Optional.of(this);
        }
        return findByPathRecurse(path, this.children);
    }

    @SuppressWarnings("unchecked")
    public StorageNode<T> partitionedPath()
    {
        String rootedPath = path.toString();
        if (!rootedPath.endsWith("/")) {
            rootedPath += "/";
        }
        path = (T) rootedPath;
        return this;
    }

    @Override
    public String toString()
    {
        return "StorageNode{" +
                "data=" + data +
                ", path=" + path +
                '}';
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof StorageNode)) {
            return false;
        }
        return this.hashCode() == other.hashCode();
    }

    // helpers
    private Optional<StorageNode<T>> findByPathRecurse(String path, TreeSet<StorageNode<T>> children)
    {
        for (StorageNode<T> node : children) {
            if (node.path.equals(path)) {
                return Optional.of(node);
            }
        }

        for (StorageNode<T> node : children) {
            Optional<StorageNode<T>> optionalNode = findByPathRecurse(path, node.children);
            if (optionalNode.isPresent()) {
                return Optional.of(optionalNode.get());
            }
        }
        return Optional.empty();
    }

    @Override
    public int compareTo(StorageNode<T> o)
    {
        return this.path.compareTo(o.path);
    }
}
