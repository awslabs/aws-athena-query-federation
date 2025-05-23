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
package com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;

import java.util.Arrays;
import java.util.List;

/**
 * This class describes Query Passthrough Signature;
 * Schema; is where the function will reside in the catalog attaching this namespace
 * Name; is the table function name of the QPT;
 * Arguments; list of all arguments that this QPT is expecting to have
 *
 */
public enum QueryPassthrough {
    QUERY_PASSTHROUGH_SCHEMA("query_passthrough_schema"),
    QUERY_PASSTHROUGH_NAME("query_passthrough_name"),
    QUERY_PASSTHROUGH_ARGUMENTS("query_passthrough_arguments");
    private final String value;

    QueryPassthrough(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    public final OptimizationSubType withSchema(String schema)
    {
        return new OptimizationSubType(getValue(), Arrays.asList(schema));
    }

    public final OptimizationSubType withName(String name)
    {
        return new OptimizationSubType(getValue(), Arrays.asList(name));
    }

    public final OptimizationSubType withArguments(List<String> arguments)
    {
        return new OptimizationSubType(getValue(), arguments);
    }
}
