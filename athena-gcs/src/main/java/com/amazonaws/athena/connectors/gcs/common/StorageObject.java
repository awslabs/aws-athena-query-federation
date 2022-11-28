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

import java.util.List;

/**
 * Represents a storage object's meta data, usually for to represent a Table in Athena. It also indicates whether the table is partitioned.
 * When the table is partitioned, it also contains a list of partition column name list (currently not in used as of this implementation)
 */
public class StorageObject
{
    private String tableName;
    private String objectName;
    private boolean partitioned;
    private List<String> partitionedColumns;

    public StorageObject(String tableName, String objectName, boolean partitioned, List<String> partitionedColumns)
    {
        this.tableName = tableName;
        this.objectName = objectName;
        this.partitioned = partitioned;
        this.partitionedColumns = partitionedColumns;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    public String getObjectName()
    {
        return objectName;
    }

    public void setObjectName(String objectName)
    {
        this.objectName = objectName;
    }

    public void setPartitioned(boolean partitioned)
    {
        this.partitioned = partitioned;
    }

    public List<String> getPartitionedColumns()
    {
        return partitionedColumns;
    }

    public void setPartitionedColumns(List<String> partitionedColumns)
    {
        this.partitionedColumns = partitionedColumns;
    }

    @Override
    public String toString()
    {
        return "StorageObject{" +
                "objectName='" + tableName + '\'' +
                ", partitioned=" + partitioned +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String tabletName;
        private String objectName;
        private boolean partitioned;
        private List<String> partitionedColumns;

        public Builder setTabletName(final String tabletName)
        {
            this.tabletName = tabletName;
            return this;
        }

        public Builder setObjectName(String objectName)
        {
            this.objectName = objectName;
            return this;
        }

        public Builder setPartitioned(final boolean partitioned)
        {
            this.partitioned = partitioned;
            return this;
        }

        public Builder partitionedColumns(List<String> partitionedColumns)
        {
            this.partitionedColumns = partitionedColumns;
            return this;
        }

        public StorageObject build()
        {
            return new StorageObject(this.tabletName, this.objectName,  this.partitioned,
                    this.partitionedColumns == null
                            ? List.of()
                            : this.partitionedColumns);
        }
    }
}
