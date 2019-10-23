package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

public class ListTablesResponse
        extends MetadataResponse
{
    private final Collection<TableName> tables;

    @JsonCreator
    public ListTablesResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("tables") Collection<TableName> tables)
    {
        super(MetadataRequestType.LIST_TABLES, catalogName);
        requireNonNull(tables, "tables is null");
        this.tables = Collections.unmodifiableCollection(tables);
    }

    public Collection<TableName> getTables()
    {
        return tables;
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
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        ListTablesResponse that = (ListTablesResponse) o;

        return CollectionsUtils.equals(this.tables, that.tables) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tables, getRequestType(), getCatalogName());
    }
}
