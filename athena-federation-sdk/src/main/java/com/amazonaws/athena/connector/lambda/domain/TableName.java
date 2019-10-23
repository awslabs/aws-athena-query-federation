package com.amazonaws.athena.connector.lambda.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class TableName
{
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public TableName(@JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {

        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

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
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

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
