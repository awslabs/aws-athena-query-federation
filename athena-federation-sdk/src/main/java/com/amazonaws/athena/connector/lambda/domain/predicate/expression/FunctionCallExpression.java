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
package com.amazonaws.athena.connector.lambda.domain.predicate.expression;

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class FunctionCallExpression
        extends FederationExpression
{
    private final FunctionName functionName;
    private final List<FederationExpression> arguments;

    @JsonCreator
    public FunctionCallExpression(
            @JsonProperty("type") ArrowType type,
            @JsonProperty("functionName") FunctionName functionName,
            @JsonProperty("arguments") List<FederationExpression> arguments)
    {
        super(type);
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.arguments = new ArrayList<>(requireNonNull(arguments, "arguments is null"));
    }

    @JsonProperty("functionName")
    public FunctionName getFunctionName()
    {
        return functionName;
    }

    @JsonProperty("arguments")
    public List<FederationExpression> getArguments()
    {
        return arguments;
    }

    @Override
    @JsonProperty("children")
    public List<? extends FederationExpression> getChildren()
    {
        return arguments;
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
        FunctionCallExpression call = (FunctionCallExpression) o;
        return Objects.equals(functionName, call.functionName) &&
                Objects.equals(arguments, call.arguments) &&
                Objects.equals(getType(), call.getType());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, arguments, getType());
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", FunctionCallExpression.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("functionName=" + functionName)
                .add("arguments=" + arguments)
                .toString();
    }
}
