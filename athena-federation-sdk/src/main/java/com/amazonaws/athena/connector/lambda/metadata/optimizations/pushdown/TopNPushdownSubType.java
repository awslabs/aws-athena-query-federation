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

public enum TopNPushdownSubType
        implements PushdownSubTypes
{
    SUPPORTS_ORDER_BY("SUPPORTS_ORDER_BY");

    private String subType;

    @Override
    public String getSubType()
    {
        return subType;
    }

    TopNPushdownSubType(String subType)
    {
        this.subType = subType;
    }
}
