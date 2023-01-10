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
package com.amazonaws.athena.connector.lambda.domain.predicate.aggregation;

import com.amazonaws.athena.connector.lambda.domain.predicate.functions.FunctionName;

public enum AggregationFunctions
{
    SUM(new FunctionName("$sum")),
    MAX(new FunctionName("$max")),
    MIN(new FunctionName("$min")),
    COUNT(new FunctionName("$count")), // not yet supported, just for the future
    AVG(new FunctionName("$avg")); // not yet supported, just for the future

    FunctionName functionName;

    public FunctionName getFunctionName()
    {
        return this.functionName;
    }

    AggregationFunctions(FunctionName functionName)
    {
        this.functionName = functionName;
    }

    public static AggregationFunctions fromFunctionName(FunctionName functionName)
    {
        for (AggregationFunctions function : AggregationFunctions.values()) {
            if (function.getFunctionName().equals(functionName)) {
                return function;
            }
        }
        return null;
    }
}
