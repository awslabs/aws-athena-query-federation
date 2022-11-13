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

import com.amazonaws.athena.storage.StorageDatasource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TreeTraversalContext
{
    private boolean hasParent;
    private boolean includeFile;
    private int maxDepth;
    private int partitionDepth = -1;
    private StorageDatasource storageDatasource;
    private final List<FilterExpression> filters = new ArrayList<>();

    public TreeTraversalContext(boolean hasParent, boolean includeFile, int maxDepth, int partitionDepth, StorageDatasource storageDatasource)
    {
        this.hasParent = hasParent;
        this.includeFile = includeFile;
        this.maxDepth = maxDepth;
        this.partitionDepth = partitionDepth;
        this.storageDatasource = storageDatasource;
    }

    public boolean isIncludeFile()
    {
        return includeFile;
    }

    public void setIncludeFile(boolean includeFile)
    {
        this.includeFile = includeFile;
    }

    public int getMaxDepth()
    {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth)
    {
        this.maxDepth = maxDepth;
    }

    public StorageDatasource getStorageDatasource()
    {
        return storageDatasource;
    }

    public void setStorageDatasource(StorageDatasource storageDatasource)
    {
        this.storageDatasource = storageDatasource;
    }

    public boolean hasParent()
    {
        return hasParent;
    }

    public void setHasParent(boolean hasParent)
    {
        this.hasParent = hasParent;
    }

    public String[] normalizePaths(String[] paths)
    {
        int currDepth = hasParent ? paths.length - 1 : paths.length;
        if (maxDepth > 0 && currDepth <= maxDepth) {
            return Arrays.copyOfRange(paths, 0, maxDepth);
        }
        return paths;
    }

    public int getPartitionDepth()
    {
        return partitionDepth;
    }

    public void setPartitionDepth(int partitionDepth)
    {
        this.partitionDepth = partitionDepth;
    }

    public void addFiler(FilterExpression filter)
    {
        this.filters.add(filter);
    }

    public void addAllFilers(List<FilterExpression> filters)
    {
        this.filters.addAll(filters);
    }

    public boolean isFile(String bucket, String path)
    {
        return !storageDatasource.getStorageProvider().isDirectory(bucket, path);
    }

    public boolean isPartitioned(int depth, String name)
    {
        if (partitionDepth > -1 && depth >= partitionDepth) {
            boolean partitioned = PartitionUtil.isPartitionFolder(name);
            if (partitioned) {
                Optional<FieldValue> optionalFieldValue = FieldValue.from(name);
                if (optionalFieldValue.isPresent()) {
                    return matchAnyFilter(optionalFieldValue.get());
                }
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "TreeTraversalContext{" +
                "includeFile=" + includeFile +
                ", maxDepth=" + maxDepth +
                ", storageDatasource=" + storageDatasource +
                '}';
    }

    // helpers
    private boolean matchAnyFilter(FieldValue fieldValue)
    {
        if (filters.isEmpty()) {
            return true;
        }

        for (FilterExpression expression : filters) {
            if (expression.columnName().equals(fieldValue.getField())
                    && expression.filterValue().toString().equals(fieldValue.getValue())) {
                return true;
            }
        }
        return false;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean hasParent;
        private boolean includeFile;
        private int maxDepth;
        private int partitionDepth = -1;
        private StorageDatasource storageDatasource;

        private Builder()
        {
        }

        public Builder hasParent(boolean hasParent)
        {
            this.hasParent = hasParent;
            return this;
        }

        public Builder includeFile(boolean includeFile)
        {
            this.includeFile = includeFile;
            return this;
        }

        public Builder maxDepth(int maxDepth)
        {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder partitionDepth(int partitionDepth)
        {
            this.partitionDepth = partitionDepth;
            return this;
        }

        public Builder storageDatasource(StorageDatasource storageDatasource)
        {
            this.storageDatasource = storageDatasource;
            return this;
        }

        public TreeTraversalContext build()
        {
            return new TreeTraversalContext(hasParent, includeFile, maxDepth, this.partitionDepth, storageDatasource);
        }
    }
}
