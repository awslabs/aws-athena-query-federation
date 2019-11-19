package com.amazonaws.athena.connector.lambda.examples;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * Example user-defined-handler that demonstrates how to write user defined functions. We don't recommend that you use
 * this class directly. Instead, please create new classes that extends
 * {@link UserDefinedFunctionHandler UserDefinedFunctionHandler} and copy-paste codes as needed.
 */
public class ExampleUserDefinedFunctionHandler
        extends UserDefinedFunctionHandler
{
    //Used to aid in diagnostic logging
    private static final String SOURCE_TYPE = "custom";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExampleUserDefinedFunctionHandler()
    {
        super(SOURCE_TYPE);
    }

    /**
     * This UDF takes 2 integers, returns the produce of the 2 integers.
     *
     * @param factor1
     * @param factor2
     * @return product
     */
    public Integer multiply(Integer factor1, Integer factor2)
    {
        return factor1 * factor2;
    }

    /**
     * This UDF takes in a list of String as input, concatenate every element in the list and returns.
     * @param list
     * @return
     */
    public String concatenate(List<String> list)
    {
        StringBuilder sb = new StringBuilder();
        for (String value : list) {
            sb.append(value);
        }
        return sb.toString();
    }

    /**
     * This UDF writes the complext struct type value to Json string.
     * @param struct
     * @return Json string of the struct object
     */
    public String to_json(Map<String, Object> struct)
    {
        try {
            return objectMapper.writeValueAsString(struct);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This UDF takes in a Long value, and returns a default value if the input value is not set.
     * @param value
     * @return original value or 0 if value is not set (null)
     */
    public Long get_default_value_if_null(Long value)
    {
        return value == null ? 0 : value;
    }
}
