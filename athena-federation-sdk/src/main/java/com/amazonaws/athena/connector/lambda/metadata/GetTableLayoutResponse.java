package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;
import java.util.Collections;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class GetTableLayoutResponse
        extends MetadataResponse
{
    private final TableName tableName;
    private final Block partitions;
    private final Set<String> partitionCols;

    @JsonCreator
    public GetTableLayoutResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("partitions") Block partitions,
            @JsonProperty("partitionCols") Set<String> partitionCols)
    {
        super(MetadataRequestType.GET_TABLE_LAYOUT, catalogName);
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");
        requireNonNull(partitionCols, "partitionCols is null");
        this.tableName = tableName;
        this.partitions = partitions;
        this.partitionCols = Collections.unmodifiableSet(partitionCols);
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
    public Set<String> getPartitionCols()
    {
        return partitionCols;
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
                .add("partitionCols", partitionCols)
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .add("partitions", partitions)
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
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        GetTableLayoutResponse that = (GetTableLayoutResponse) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.partitions, that.partitions) &&
                Objects.equal(this.partitionCols, that.partitionCols) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, partitions, partitionCols, getRequestType(), getCatalogName());
    }
}
