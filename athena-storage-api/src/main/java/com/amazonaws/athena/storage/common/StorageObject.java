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

public class StorageObject
{
    private String objectName;
    private boolean partitioned;

    public StorageObject(String objectName, boolean partitioned)
    {
        this.objectName = objectName;
        this.partitioned = partitioned;
    }

    public String getObjectName()
    {
        return objectName;
    }

    public void setObjectName(String objectName)
    {
        this.objectName = objectName;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    public void setPartitioned(boolean partitioned)
    {
        this.partitioned = partitioned;
    }

    @Override
    public String toString()
    {
        return "StorageObject{" +
                "objectName='" + objectName + '\'' +
                ", partitioned=" + partitioned +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String objectName;
        private boolean partitioned;

        public Builder setObjectName(final String objectName)
        {
            this.objectName = objectName;
            return this;
        }

        public Builder setPartitioned(final boolean partitioned)
        {
            this.partitioned = partitioned;
            return this;
        }

        public StorageObject build()
        {
            return new StorageObject(this.objectName, this.partitioned);
        }
    }
}
