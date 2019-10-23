package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

public class ListSchemasResponse
        extends MetadataResponse
{
    private final Collection<String> schemas;

    @JsonCreator
    public ListSchemasResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemas") Collection<String> schemas)
    {
        super(MetadataRequestType.LIST_SCHEMAS, catalogName);
        requireNonNull(schemas, "schemas is null");
        this.schemas = Collections.unmodifiableCollection(schemas);
    }

    public Collection<String> getSchemas()
    {
        return schemas;
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

        ListSchemasResponse that = (ListSchemasResponse) o;

        return CollectionsUtils.equals(this.schemas, that.schemas) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemas, getRequestType(), getCatalogName());
    }
}
