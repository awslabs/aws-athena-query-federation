/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.domain.predicate.aggregation;

import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FederationExpression;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.FunctionCallExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

public class AggregateFunctionClause
{
    private final List<FederationExpression> aggregateFunctions;
    private final List<String> columnNames;
    private final List<List<String>> groupingSets;

    @JsonCreator
    public AggregateFunctionClause(@JsonProperty("aggregateFunctions") List<FederationExpression> aggregateFunctions,
                                   @JsonProperty("columnNames") List<String> columnNames,
                                   @JsonProperty("groupingSets") List<List<String>> groupingSets)
    {
        this.aggregateFunctions = aggregateFunctions;
        this.columnNames = columnNames;
        this.groupingSets = groupingSets;
    }

    @JsonProperty("aggregateFunctions")
    public List<FederationExpression> getAggregateFunctions()
    {
        return aggregateFunctions;
    }

    @JsonProperty("columnNames")
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty("groupingSets")
    public List<List<String>> getGroupingSets()
    {
        return groupingSets;
    }

    public static List<AggregateFunctionClause> emptyAggregateFunctionClause()
    {
        return Collections.emptyList();
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
        AggregateFunctionClause call = (AggregateFunctionClause) o;
        return Objects.equals(aggregateFunctions, call.aggregateFunctions) &&
                Objects.equals(columnNames, call.columnNames) &&
                Objects.equals(groupingSets, call.groupingSets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregateFunctions, columnNames, groupingSets);
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", FunctionCallExpression.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("aggregateFunctions=" + aggregateFunctions)
                .add("columnNames=" + columnNames)
                .add("groupingSets=" + groupingSets)
                .toString();
    }
}
