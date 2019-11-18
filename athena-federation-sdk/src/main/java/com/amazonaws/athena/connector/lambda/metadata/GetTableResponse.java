package com.amazonaws.athena.connector.lambda.metadata;

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

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Represents the output of a <code>GetTable</code> operation.
 */
public class GetTableResponse
        extends MetadataResponse
{
    private final TableName tableName;
    private final Schema schema;
    private final Set<String> partitionColumns;

    /**
     * Constructs a new GetTableResponse object.
     *
     * @param catalogName The catalog name the table belongs to.
     * @param tableName The name of the table (must be lowercase).
     * @param schema The schema of the table.
     * @param partitionColumns The partition column names of the table.
     */
    @JsonCreator
    public GetTableResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("schema") Schema schema,
            @JsonProperty("partitionColumns") Set<String> partitionColumns)
    {
        super(MetadataRequestType.GET_TABLE, catalogName);
        requireNonNull(tableName, "tableName is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(partitionColumns, "partitionColumns is null");
        this.tableName = tableName;
        this.schema = schema;
        this.partitionColumns = partitionColumns;
    }

    /**
     * Constructs a new GetTableResponse object.  This constructor populates an empty set for the
     * partition column names.
     *
     * @param catalogName The catalog name the table belongs to.
     * @param tableName The name of the table that was fetched.
     * @param schema The schema of the table.
     */
    public GetTableResponse(String catalogName, TableName tableName, Schema schema)
    {
        this(catalogName, tableName, schema, Collections.emptySet());
    }

    /**
     * Returns the name of the table.
     *
     * @return The name of the table.
     */
    public TableName getTableName()
    {
        return tableName;
    }

    /**
     * Returns the schema of the table.
     *
     * @return The schema of the table.
     */
    public Schema getSchema()
    {
        return schema;
    }

    /**
     * Returns partition column names of the table.
     *
     * @return The partition column names of the table.
     */
    public Set<String> getPartitionColumns()
    {
        return Collections.unmodifiableSet(partitionColumns);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("tableName", tableName)
                .add("schema", schema)
                .add("partitionColumns", partitionColumns)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
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

        GetTableResponse that = (GetTableResponse) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.schema, that.schema) &&
                Objects.equal(this.partitionColumns, that.partitionColumns) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, schema, partitionColumns, getRequestType(), getRequestType());
    }
}
