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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class GetTableLayoutRequest
        extends MetadataRequest
{
    private final TableName tableName;
    private final Constraints constraints;
    private final Map<String, String> properties;

    @JsonCreator
    public GetTableLayoutRequest(@JsonProperty("identity") FederatedIdentity identity,
            @JsonProperty("queryId") String queryId,
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("tableName") TableName tableName,
            @JsonProperty("constraints") Constraints constraints,
            @JsonProperty("properties") Map<String, String> properties)
    {
        super(identity, MetadataRequestType.GET_TABLE_LAYOUT, queryId, catalogName);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraints = requireNonNull(constraints, "constraints is null");
        this.properties = requireNonNull(properties, "properties is null");
    }

    public TableName getTableName()
    {
        return tableName;
    }

    public Constraints getConstraints()
    {
        return constraints;
    }

    /**
     * Contains the properties associated with this table as provided by the Schema on GetTableResult.
     */
    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public void close()
            throws Exception
    {
        for (ValueSet next : constraints.getSummary().values()) {
            next.close();
        }
        constraints.close();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        GetTableLayoutRequest that = (GetTableLayoutRequest) o;

        return Objects.equal(this.tableName, that.tableName) &&
                Objects.equal(this.constraints, that.constraints) &&
                Objects.equal(this.properties, that.properties) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName, constraints, properties, getRequestType(), getCatalogName());
    }
}

