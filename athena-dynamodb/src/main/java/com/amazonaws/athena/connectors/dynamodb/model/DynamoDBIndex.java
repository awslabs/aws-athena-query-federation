/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 - 2021 Amazon Web Services
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

import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DynamoDBIndex
{
    private final String name;
    private final String hashKey;
    private final Optional<String> rangeKey;
    private final ProjectionType projectionType;
    private final List<String> projectionAttributeNames;

    public DynamoDBIndex(String name,
                         String hashKey,
                         Optional<String> rangeKey,
                         ProjectionType projectionType,
                         List<String> projectionAttributeNames)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.hashKey = requireNonNull(hashKey, "hashKey is null");
        this.rangeKey = requireNonNull(rangeKey, "rangeKey is null");
        this.projectionType = requireNonNull(projectionType, "projectionType is null");
        this.projectionAttributeNames = requireNonNull(projectionAttributeNames, "knownAttributeDefinitions is null");
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

    public ProjectionType getProjectionType()
    {
        return projectionType;
    }

    public List<String> getProjectionAttributeNames()
    {
        return projectionAttributeNames;
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

        DynamoDBIndex other = (DynamoDBIndex) obj;
        return Objects.equals(this.name, other.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
