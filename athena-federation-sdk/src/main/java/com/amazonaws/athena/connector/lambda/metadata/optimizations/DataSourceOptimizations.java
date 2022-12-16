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
package com.amazonaws.athena.connector.lambda.metadata.optimizations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum DataSourceOptimizations
{
    SUPPORTS_AGGREGATE_FUNCTIONS("supports_aggregate_functions") 
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof AggregationPushdownSubType)) {
                throw new IllegalArgumentException("Aggregation Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_AGGREGATE_FUNCTIONS.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_LIMIT_PUSHDOWN("supports_limit_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof LimitPushdownSubType)) {
                throw new IllegalArgumentException("Limit Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_LIMIT_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_TOP_N_PUSHDOWN("supports_top_n_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof TopNPushdownSubType)) {
                throw new IllegalArgumentException("TopN Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_TOP_N_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_FILTER_PUSHDOWN("supports_filter_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof FilterPushdownSubType)) {
                throw new IllegalArgumentException("Filter Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_FILTER_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN("supports_complex_expression_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownsubTypes -> pushdownsubTypes instanceof ComplexExpressionPushdownSubType)) {
                throw new IllegalArgumentException("Complex Expression Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_PROJECTION_PUSHDOWN("supports_projection_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof ProjectionPushdownSubType)) {
                throw new IllegalArgumentException("Projection Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_PROJECTION_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_JOIN_PUSHDOWN("supports_join_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof JoinPushdownSubType)) {
                throw new IllegalArgumentException("Join Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_JOIN_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_TABLE_FUNCTION("supports_table_function")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof TableFunctionPushdownSubType)) {
                throw new IllegalArgumentException("TableFunction Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_TABLE_FUNCTION.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    },
    SUPPORTS_SAMPLE_PUSHDOWN("supports_sample_pushdown")
    {
        public Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof SamplePushdownSubType)) {
                throw new IllegalArgumentException("Sample Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return Map.of(SUPPORTS_SAMPLE_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(PushdownSubTypes::getSubType).collect(Collectors.toList()));
        }
    };

    private final String optimization;

    DataSourceOptimizations(String optimization)
    {
        this.optimization = optimization;
    }

    public String getOptimization()
    {
        return optimization;
    }

    public abstract Map<String, List<String>> withSupportedSubTypes(PushdownSubTypes... subTypesList);
}
