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

import static java.util.Objects.requireNonNull;

public class StoragePartition
{
    private String bucketName;
    private String location;

    // Jackson uses this constructor
    @SuppressWarnings("unused")
    public StoragePartition()
    {
    }

    public StoragePartition(String bucketName, String location)
    {
        this.bucketName = requireNonNull(bucketName, "Bucket name can't be null");
        this.location = requireNonNull(location, "Location can't be null");
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public void setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
    }

    public String getLocation()
    {
        return location;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public String toString()
    {
        return "StoragePartition{" +
                "bucketName='" + bucketName + '\'' +
                ", location='" + location + '\'' +
                '}';
    }

    // Builder
    public static class Builder
    {
        private String bucketName;
        private String location;

        private Builder()
        {
        }

        public Builder bucketName(String bucketName)
        {
            this.bucketName = bucketName;
            return this;
        }

        public Builder location(String location)
        {
            this.location = location;
            return this;
        }

        public StoragePartition build()
        {
            return new StoragePartition(this.bucketName, this.location);
        }
    }
}
