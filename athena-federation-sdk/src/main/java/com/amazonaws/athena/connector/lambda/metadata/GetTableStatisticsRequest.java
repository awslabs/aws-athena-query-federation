/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connector.lambda.metadata;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GetTableStatisticsRequest extends MetadataRequest
{
    private TableName tableName;

    public GetTableStatisticsRequest(@JsonProperty("identity") FederatedIdentity identity,
                                     @JsonProperty("queryId") String queryId,
                                     @JsonProperty("catalogName") String catalogName,
                                     @JsonProperty("tableName") TableName tableName)
    {
        super(identity, MetadataRequestType.GET_TABLE_STATISTICS, queryId, catalogName);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public TableName getTableName()
    {
        return tableName;
    }

    @Override
    public void close() throws Exception
    {}

    @Override
    public String toString()
    {
        return "GetTableStatisticsRequest{" +
                "tableName=" + tableName +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetTableStatisticsRequest that = (GetTableStatisticsRequest) o;
        return Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tableName);
    }
}
