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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class VariableExpression
        extends FederationExpression
{
    private final String columnName;

    @JsonCreator
    public VariableExpression(@JsonProperty("columnName") String columnName,
                              @JsonProperty("type") ArrowType type)
    {
        super(type);
        this.columnName = requireNonNull(columnName, "columnName is null");

        if (columnName.isEmpty()) {
            throw new IllegalArgumentException("columnName is empty");
        }
    }

    @JsonProperty("columnName")
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public List<? extends FederationExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, getType());
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

        VariableExpression that = (VariableExpression) o;
        return Objects.equals(columnName, that.columnName) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return columnName + "::" + getType();
    }
}
