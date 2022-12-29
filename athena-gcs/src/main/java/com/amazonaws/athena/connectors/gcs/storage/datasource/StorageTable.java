/*-
 * #%L
 * athena-hive
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
package com.amazonaws.athena.connectors.gcs.storage.datasource;

import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@VisibleForTesting
public class StorageTable
{
    private final String databaseName;
    private final String tableName;
    private final Map<String, String> parameters = new HashMap<>();
    private final List<Field> fields = new ArrayList<>();
    private final boolean partitioned;

    /**
     * Constructor to instantiate this instance with given parameters along with fields (with types) in the table
     *
     * @param databaseName Name of the database
     * @param tableName    Name of the table
     * @param parameters   Additional properties (if any)
     * @param fields       List fields
     * @param partitioned  Indicates whether the table is partitioned
     */
    public StorageTable(String databaseName, String tableName, Map<String, String> parameters,
                        List<Field> fields, boolean partitioned)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.parameters.putAll(parameters);
        this.fields.addAll(fields);
        this.partitioned = partitioned;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public Map<String, String> getParameters()
    {
        return new HashMap<>(parameters);
    }

    public List<Field> getFields()
    {
        return new ArrayList<>(fields);
    }

    public String getTableName()
    {
        return tableName;
    }

    public boolean isPartitioned()
    {
        return partitioned;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A Builder to help build {StorageTable instance
     * All setters are fluent-styled
     */
    public static class Builder
    {
        private String databaseName;
        private String tableName;
        private final List<Field> fieldList = new ArrayList<>();
        private boolean partitioned;

        private final Map<String, String> parameters;

        private Builder()
        {
            parameters = new HashMap<>();
        }

        public Builder setDatabaseName(String databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setFieldList(List<Field> fieldList)
        {
            this.fieldList.addAll(fieldList);
            return this;
        }

        public Builder setParameter(String name, String value)
        {
            this.parameters.put(name, value);
            return this;
        }

        public Builder partitioned(boolean partitioned)
        {
            this.partitioned = partitioned;
            return this;
        }

        /**
         * Instantiates an instance of StorageTable
         *
         * @return An instance of StorageTable
         */
        public StorageTable build()
        {
            String databaseName = requireNonNull(this.databaseName, "Database name was not set");
            String tableName = requireNonNull(this.tableName, "Table name was not set");
            List<Field> fields = requireNonNull(this.fieldList, "Data column(s) was not set");
            return new StorageTable(databaseName, tableName, this.parameters, fields, this.partitioned);
        }
    }
}
