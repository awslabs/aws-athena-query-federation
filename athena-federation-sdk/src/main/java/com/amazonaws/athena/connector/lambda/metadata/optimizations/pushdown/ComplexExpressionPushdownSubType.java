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
package com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum ComplexExpressionPushdownSubType
        implements PushdownSubTypes
{
    SUPPORTED_FUNCTION_EXPRESSION_TYPES("supported_function_expression_types")
    {
        @Override
        public SubTypeProperties withSubTypeProperties(String... properties)
        {
            if (properties.length == 0) {
                throw new IllegalArgumentException("Connectors that support function expressions must provide a list of supported functions. See Documentation for more details");
            }
            return new SubTypeProperties(getSubType(), Arrays.asList(properties));
        }
    };

    private String subType;

    @Override
    public String getSubType()
    {
        return subType;
    }

    ComplexExpressionPushdownSubType(String subType)
    {
        this.subType = subType;
    }

    public SubTypeProperties withSubTypeProperties(String... ignored)
    {
        return new SubTypeProperties(subType, Collections.emptyList());
    }

    public static class SubTypeProperties implements PushdownSubTypes
    {
        private final String propertyName;
        private final List<String> propertyValues;

        public SubTypeProperties(String propertyName, List<String> propertyValues)
        {
            this.propertyName = propertyName;
            this.propertyValues = propertyValues;
        }

        @Override
        public String getSubType()
        {
            return propertyName;
        }

        @Override
        public List<String> getProperties()
        {
            return propertyValues;
        }
    }
}
