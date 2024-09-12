/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class HbaseQueryPassthrough implements QueryPassthroughSignature
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseQueryPassthrough.class);

    // Constant value representing the name of the query.
    public static final String NAME = "query";

    // Constant value representing the domain of the query.
    public static final String SCHEMA_NAME = "system";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String DATABASE = "DATABASE";
    public static final String COLLECTION = "COLLECTION";
    public static final String FILTER = "FILTER";

    @Override
    public String getFunctionSchema()
    {
        return SCHEMA_NAME;
    }

    @Override
    public String getFunctionName()
    {
        return NAME;
    }

    @Override
    public List<String> getFunctionArguments()
    {
        return Arrays.asList(DATABASE, COLLECTION, FILTER);
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }
    /**
     * verify that the arguments returned by the engine are the same that the connector defined.
     * In the parent class verify function, all argument values are mandatory. However, in the overridden function,
     * we've removed the condition that checks whether the HBase FILTER argument value is empty. This modification
     * allows for the possibility of an empty FILTER argument value without triggering an exception.
     * @param engineQptArguments
     * @throws IllegalArgumentException
     */
    @Override
    public void verify(Map<String, String> engineQptArguments)
            throws IllegalArgumentException
    {
        //High-level Query Passthrough Function Argument verification
        //First verifies that these arguments belong to this specific function
        if (!verifyFunctionSignature(engineQptArguments)) {
            throw new IllegalArgumentException("Function Signature doesn't match implementation's");
        }
        //Ensuring the arguments received from the engine are the one defined by the connector
        for (String argument : this.getFunctionArguments()) {
            if (!engineQptArguments.containsKey(argument)) {
                throw new IllegalArgumentException("Missing Query Passthrough Argument: " + argument);
            }
        }
    }
}
