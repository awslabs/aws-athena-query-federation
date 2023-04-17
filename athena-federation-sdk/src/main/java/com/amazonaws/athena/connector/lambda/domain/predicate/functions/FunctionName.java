package com.amazonaws.athena.connector.lambda.domain.predicate.functions;

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Represents a fully qualified FunctionName.
 */
public class FunctionName
{
    private final String functionName;

    /**
     * Constructs a fully qualified FunctionName.
     *
     * @param functionName The name of the function.
     */
    @JsonCreator
    public FunctionName(@JsonProperty("functionName") String functionName)
    {
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    /**
     * Gets the name of the function.
     *
     * @return A String containing the name of the function.
     */
    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("functionName", functionName)
                .toString();
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

        FunctionName that = (FunctionName) o;

        return Objects.equal(this.functionName, that.functionName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(functionName);
    }
}
