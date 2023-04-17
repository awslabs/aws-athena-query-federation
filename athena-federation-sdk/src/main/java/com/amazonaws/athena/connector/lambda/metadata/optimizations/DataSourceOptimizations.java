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

import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.PushdownSubTypes;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public enum DataSourceOptimizations
{
    SUPPORTS_LIMIT_PUSHDOWN("supports_limit_pushdown")
    {
        public Map.Entry<String, List<OptimizationSubType>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof LimitPushdownSubType)) {
                throw new IllegalArgumentException("Limit Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return new SimpleImmutableEntry<String, List<OptimizationSubType>>(SUPPORTS_LIMIT_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(pushdownSubTypes -> new OptimizationSubType(pushdownSubTypes.getSubType(), pushdownSubTypes.getProperties())).collect(Collectors.toList()));
        }
    },
    SUPPORTS_TOP_N_PUSHDOWN("supports_top_n_pushdown")
    {
        public Map.Entry<String, List<OptimizationSubType>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof TopNPushdownSubType)) {
                throw new IllegalArgumentException("TopN Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return new SimpleImmutableEntry<String, List<OptimizationSubType>>(SUPPORTS_TOP_N_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(pushdownSubTypes -> new OptimizationSubType(pushdownSubTypes.getSubType(), pushdownSubTypes.getProperties())).collect(Collectors.toList()));
        }
    },
    SUPPORTS_FILTER_PUSHDOWN("supports_filter_pushdown")
    {
        public Map.Entry<String, List<OptimizationSubType>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownSubTypes -> pushdownSubTypes instanceof FilterPushdownSubType)) {
                throw new IllegalArgumentException("Filter Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return new SimpleImmutableEntry<String, List<OptimizationSubType>>(SUPPORTS_FILTER_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(pushdownSubTypes -> new OptimizationSubType(pushdownSubTypes.getSubType(), pushdownSubTypes.getProperties())).collect(Collectors.toList()));
        }
    },
    SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN("supports_complex_expression_pushdown")
    {
        public Map.Entry<String, List<OptimizationSubType>> withSupportedSubTypes(PushdownSubTypes... subTypesList)
        {
            if (!Arrays.stream(subTypesList).allMatch(pushdownsubTypes -> pushdownsubTypes instanceof ComplexExpressionPushdownSubType || pushdownsubTypes instanceof ComplexExpressionPushdownSubType.SubTypeProperties)) {
                throw new IllegalArgumentException("Complex Expression Pushdown Optimization must contain valid pushdown subtypes.");
            }
            return new SimpleImmutableEntry<String, List<OptimizationSubType>>(SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.getOptimization(), Arrays.stream(subTypesList).map(pushdownSubTypes -> new OptimizationSubType(pushdownSubTypes.getSubType(), pushdownSubTypes.getProperties())).collect(Collectors.toList()));
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

    public abstract Map.Entry<String, List<OptimizationSubType>> withSupportedSubTypes(PushdownSubTypes... subTypesList);
}
