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
package com.amazonaws.athena.storage;

import static java.util.Objects.requireNonNull;

public class StoragePartition
{
    private String objectName;
    private String location;
    private long recordCount;

    // Jackson uses this constructor
    @SuppressWarnings("unused")
    public StoragePartition()
    {
    }

    public StoragePartition(String objectName, String location, long recordCount)
    {
        this.objectName = objectName;
        this.location = location;
        this.recordCount = recordCount;
    }

    public String getObjectName()
    {
        return objectName;
    }

    public void setObjectName(String objectName)
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

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "StoragePartition{" +
                "objectName='" + objectName + '\'' +
                ", location='" + location + '\'' +
                ", recordCount=" + recordCount +
                '}';
    }

    // Builder
    public static class Builder
    {
        private String objectName;
        private String location;
        private long recordCount;

        private Builder()
        {
        }

        public Builder objectName(String objectName)
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

        public StoragePartition build()
        {
            return new StoragePartition(this.objectName, this.location, this.recordCount);
        }
    }
}
