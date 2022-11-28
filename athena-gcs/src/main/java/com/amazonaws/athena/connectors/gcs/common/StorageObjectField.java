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

public class StorageObjectField
{
    private String columnName;
    private Integer columnIndex;

    public StorageObjectField(String columnName, Integer columnIndex)
    {
        this.columnName = requireNonNull(columnName, "columnName was null");
        this.columnIndex = requireNonNull(columnIndex, "columnIndex was null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    public void setColumnName(String columnName)
    {
        this.columnName = columnName;
    }

    public Integer getColumnIndex()
    {
        return columnIndex;
    }

    public void setColumnIndex(Integer columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    @Override
    public String toString()
    {
        return "StorageObjectField{" + "columnName='" + columnName + '\'' + ", columnIndex=" + columnIndex + '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String columnName;
        private Integer columnIndex;

        public Builder columnName(final String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder columnIndex(final Integer columnIndex)
        {
            this.columnIndex = columnIndex;
            return this;
        }

        public StorageObjectField build()
        {
            return new StorageObjectField(this.columnName, this.columnIndex);
        }
    }
}
