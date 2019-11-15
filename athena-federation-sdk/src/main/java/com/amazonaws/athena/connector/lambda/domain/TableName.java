package com.amazonaws.athena.connector.lambda.domain;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a fully qualified TableName.
 */
public class TableName
{
    //The schema name that the table belongs to.
    private final String schemaName;
    //The name of the table.
    private final String tableName;

    /**
     * Constructs a fully qualified TableName.
     *
     * @param schemaName The name of the schema that the table belongs to.
     * @param tableName The name of the table.
     */
    @JsonCreator
    public TableName(@JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    /**
     * Gets the name of the schema the table belongs to.
     *
     * @return A String containing the schema name for the table.
     */
    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    /**
     * Gets the name of the table.
     *
     * @return A String containing the name of the table.
     */
    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableName that = (TableName) o;

        return Objects.equal(this.schemaName, that.schemaName) &&
                Objects.equal(this.tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName);
    }
}
