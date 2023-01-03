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

/**
 * Used to get Storage Partition
 */
public class StoragePartition
{
    private String columnName;
    private String columnType;
    private Object columnValue;

    public StoragePartition()
    {
    }

    public String getColumnName()
    {
        return columnName;
    }

    public StoragePartition columnName(String columnName)
    {
        this.columnName = columnName;
        return this;
    }

    public StoragePartition columnType(String columnType)
    {
        this.columnType = columnType;
        return this;
    }

    public Object getColumnValue()
    {
        return columnValue;
    }

    public void columnValue(Object columnValue)
    {
        this.columnValue = columnValue;
    }

    @Override
    public String toString()
    {
        return "StoragePartition{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", columnValue=" + columnValue +
                '}';
    }
}
