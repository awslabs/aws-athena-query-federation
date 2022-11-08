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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class StoragePartition
{
    private List<String> objectName;
    private String location;
    private long recordCount;
    private List<StoragePartition> children;

    // Jackson uses this constructor
    @SuppressWarnings("unused")
    public StoragePartition()
    {
    }

    public StoragePartition(List<String> objectName, String location, Long recordCount, List<StoragePartition> children)
    {
        this.objectName = requireNonNull(objectName, "objectName was null");
        this.location = requireNonNull(location, "location was null");
        this.recordCount = requireNonNull(recordCount, "recordCount was null");
        this.children = requireNonNull(children, "children was null. However, could be empty list");
    }

    public List<String> getObjectName()
    {
        return objectName;
    }

    public void setObjectName(List<String> objectName)
    {
        this.objectName = objectName;
    }

    public String getLocation()
    {
        return location;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public void setRecordCount(long recordCount)
    {
        this.recordCount = recordCount;
    }

    public List<StoragePartition> getChildren()
    {
        return children;
    }

    public void setChildren(List<StoragePartition> children)
    {
        this.children = children;
    }

    @Override
    public String toString()
    {
        return "StoragePartition{" +
                "objectName=" + objectName +
                ", location='" + location + '\'' +
                ", recordCount=" + recordCount +
                ", children=" + children +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    // Builder
    public static class Builder
    {
        private List<String> objectName;
        private String location;
        private long recordCount;
        private List<StoragePartition> children = List.of();

        private Builder()
        {
        }

        public Builder objectName(List<String> objectName)
        {
            this.objectName = requireNonNull(objectName, "objectName can't be null");
            return this;
        }

        public Builder location(String location)
        {
            this.location = requireNonNull(location, "location can't be null");
            return this;
        }

        public Builder recordCount(Long recordCount)
        {
            this.recordCount = requireNonNull(recordCount, "recordCount can't be null");
            return this;
        }

        public Builder children(List<StoragePartition> children)
        {
            this.children = children;
            return this;
        }

        public StoragePartition build()
        {
            return new StoragePartition(this.objectName, this.location, this.recordCount, this.children);
        }
    }
}
