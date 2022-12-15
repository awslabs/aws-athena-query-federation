/*-
 * #%L
 * athena-gcs
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

import com.amazonaws.athena.connectors.gcs.UncheckedGcsConnectorException;

import static java.util.Objects.requireNonNull;

public class StorageLocation
{
    private String bucketName;
    private String location;

    public StorageLocation(String bucketName, String location)
    {
        this.bucketName = requireNonNull(bucketName, "Bucket name was null");
        this.location = requireNonNull(location, "Location name was null");
        if (!this.location.endsWith("/")) {
            this.location += "/";
        }
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

    @Override
    public String toString()
    {
        return "StorageLocation{" +
                "bucketName='" + bucketName + '\'' +
                ", location='" + location + '\'' +
                '}';
    }

    // static helper
    public static StorageLocation fromUri(String uri)
    {
        int schemeIndex = uri.indexOf("://");
        if (schemeIndex < 2) {
            throw new UncheckedGcsConnectorException("Malformed GCS URI: " + uri);
        }
        String baseLocation = uri.substring(schemeIndex + 3);
        int separatorIndex = baseLocation.indexOf("/");
        return new StorageLocation(baseLocation.substring(0, separatorIndex), baseLocation.substring(separatorIndex + 1));
    }

    // builder
    public static Builder builder()
    {
        return new Builder();
    }

    private static class Builder
    {
        private String bucketName;
        private String location;

        private Builder(){}

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

        public StorageLocation build()
        {
            return new StorageLocation(bucketName, location);
        }
    }
}
