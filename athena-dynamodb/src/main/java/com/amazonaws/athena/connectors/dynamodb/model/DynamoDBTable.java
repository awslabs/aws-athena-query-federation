/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.model;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * A model class to store table metadata in an easy to consume manner.
 */
public class DynamoDBTable
{
    private final String name;
    private final String hashKey;
    private final Optional<String> rangeKey;
    private final List<AttributeDefinition> knownAttributeDefinitions;
    private final List<DynamoDBIndex> indexes;
    private final long approxTableSizeInBytes;
    private final long approxItemCount;
    private final long provisionedReadCapacity;

    public DynamoDBTable(
            String name,
            String hashKey,
            Optional<String> rangeKey,
            List<AttributeDefinition> knownAttributeDefinitions,
            List<DynamoDBIndex> indexes,
            long approxTableSizeInBytes,
            long approxItemCount,
            long provisionedReadCapacity)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.hashKey = requireNonNull(hashKey, "hashKey is null");
        this.rangeKey = requireNonNull(rangeKey, "rangeKey is null");
        this.knownAttributeDefinitions = requireNonNull(knownAttributeDefinitions, "knownAttributeDefinitions is null");
        this.name = requireNonNull(name, "name is null");
        this.indexes = ImmutableList.copyOf(requireNonNull(indexes, "indexes is null"));
        this.approxTableSizeInBytes = approxTableSizeInBytes;
        this.approxItemCount = approxItemCount;
        this.provisionedReadCapacity = provisionedReadCapacity;
    }

    public String getName()
    {
        return name;
    }

    public String getHashKey()
    {
        return hashKey;
    }

    public Optional<String> getRangeKey()
    {
        return rangeKey;
    }

    public List<AttributeDefinition> getKnownAttributeDefinitions()
    {
        return knownAttributeDefinitions;
    }

    public List<DynamoDBIndex> getIndexes()
    {
        return indexes;
    }

    public long getApproxTableSizeInBytes()
    {
        return approxTableSizeInBytes;
    }

    public long getProvisionedReadCapacity()
    {
        return provisionedReadCapacity;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DynamoDBTable other = (DynamoDBTable) obj;
        return Objects.equals(this.name, other.name);
    }
}
