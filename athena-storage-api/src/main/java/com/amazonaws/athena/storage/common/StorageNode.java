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

import java.util.Optional;
import java.util.TreeSet;

public class StorageNode<T extends Comparable<T>> implements Comparable<StorageNode<T>>
{
    private final TreeSet<StorageNode<T>> children = new TreeSet<>();
    private StorageNode<T> parent = null;
    private final T data;
    private final T path;
    private int index = 0;
    private final TreeTraversalContext context;

    public StorageNode(T data, T path, TreeTraversalContext context)
    {
        this.data = data;
        this.path = path;
        this.context = context;
    }

    public StorageNode(T data, T path, StorageNode<T> parent, TreeTraversalContext context)
    {
        this.data = data;
        this.path = path;
        this.parent = parent;
        this.context = context;
    }

    public TreeSet<StorageNode<T>> getChildren()
    {
        return children;
    }

    public void setParent(StorageNode<T> parent)
    {
        parent.addChild(this);
        this.parent = parent;
    }

    public StorageNode<T> addChild(T data, T path, TreeTraversalContext context)
    {
        StorageNode<T> child = new StorageNode<>(data, path, context);
        child.parent = this;
        child.index = this.children.size();
        this.children.add(child);
        return child;
    }

    public void addChild(StorageNode<T> child)
    {
        if (child.parent == null) {
            child.parent = this;
        }
        this.children.add(child);
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

    public void removeParent()
    {
        this.parent = null;
    }

    public boolean isChild(String path)
    {
        return ((String) this.path).isBlank() || this.path.equals("/") || this.path.equals(path);
    }

    public Optional<StorageNode<T>> findByPath(String path)
    {
        if (this.path.equals(path)) {
            return Optional.of(this);
        }
        return findByPathRecurse(path, this.children);
    }

    @Override
    public String toString()
    {
        return (String) path;
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
