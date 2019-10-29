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

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;

import static java.util.Objects.requireNonNull;

public class GetTableLayoutResponse
        extends MetadataResponse
{
    private final TableName tableName;
    private final Block partitions;

    @JsonCreator
    public GetTableLayoutResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("partitions") Block partitions)
    {
        super(MetadataRequestType.GET_TABLE_LAYOUT, catalogName);
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");
        this.tableName = tableName;
        this.partitions = partitions;
    }

    @JsonProperty
    public TableName getTableName()
    {
        return tableName;
    }

    @Transient
    public Schema getSchema()
    {
        return partitions.getSchema();
    }

    @JsonProperty
    public Block getPartitions()
    {
        return partitions;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("tableName", tableName)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        partitions.close();
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

        GetTableLayoutResponse that = (GetTableLayoutResponse) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.partitions, that.partitions) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, partitions, getRequestType(), getCatalogName());
    }
}
