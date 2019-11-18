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

import com.amazonaws.athena.connector.lambda.CollectionsUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Collection;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

/**
 * Represents the output of a <code>ListSchemas</code> operation.
 */
public class ListSchemasResponse
        extends MetadataResponse
{
    private final Collection<String> schemas;

    /**
     * Constructs a new ListSchemasResponse object.
     *
     * @param catalogName The catalog name that schemas were listed for.
     * @param schemas The list of schema names (they all must be lowercase).
     */
    @JsonCreator
    public ListSchemasResponse(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemas") Collection<String> schemas)
    {
        super(MetadataRequestType.LIST_SCHEMAS, catalogName);
        requireNonNull(schemas, "schemas is null");
        this.schemas = Collections.unmodifiableCollection(schemas);
    }

    /**
     * Returns the list of schema names.
     *
     * @return The list of schema names.
     */
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
    public String toString()
    {
        return "ListSchemasResponse{" +
                "schemas=" + schemas +
                '}';
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
