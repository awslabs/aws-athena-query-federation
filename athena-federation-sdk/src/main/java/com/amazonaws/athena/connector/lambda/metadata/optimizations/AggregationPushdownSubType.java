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

public enum AggregationPushdownSubType
        implements PushdownSubTypes
{
    SUPPORTS_MAX_PUSHDOWN("supports_max_pushdown"),
    SUPPORTS_AVG_PUSHDOWN("supports_avg_pushdown"),
    SUPPORTS_MIN_PUSHDOWN("supports_min_pushdown");

    private String subType;

    AggregationPushdownSubType(String subtype)
    {
        this.subType = subtype;
    }

    @Override
    public String getSubType()
    {
        return subType;
    }
}
