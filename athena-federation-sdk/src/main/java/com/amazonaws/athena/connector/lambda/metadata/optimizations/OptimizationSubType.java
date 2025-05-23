/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.metadata.optimizations;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.util.List;

public class OptimizationSubType
{
    private final String subType;
    private final List<String> properties;

    @JsonCreator
    public OptimizationSubType(@JsonProperty("subType") String subType,
                               @JsonProperty("properties") List<String> properties)
    {
        this.subType = subType;
        this.properties = properties;
    }

    @JsonProperty
    public String getSubType()
    {
        return subType;
    }

    @JsonProperty
    public List<String> getProperties()
    {
        return properties;
    }

    @Override
    public String toString()
    {
        return "OptimizationSubType{" +
                "optimization=" + subType +
                ", properties=" + properties +
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

        OptimizationSubType that = (OptimizationSubType) o;

        return Objects.equal(this.subType, that.subType) &&
                Objects.equal(this.properties, that.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(subType, properties);
    }
}
