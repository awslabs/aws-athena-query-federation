/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.cloudwatch.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class CloudwatchQueryPassthrough implements QueryPassthroughSignature
{
    private static final String SCHEMA_NAME = "system";
    private static final String NAME = "query";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String ENDTIME = "ENDTIME";
//    public static final String LIMIT = "LIMIT";
//    public static final String LOGGROUPIDENTIFIERS = "LOGGROUPIDENTIFIERS";
    public static final String LOGGROUPNAME = "LOGGROUPNAME";
//    public static final String LOGGROUPNAMES = "LOGGROUPNAMES";
    public static final String QUERYSTRING = "QUERYSTRING";
    public static final String STARTTIME = "STARTTIME";


    private static final Logger LOGGER = LoggerFactory.getLogger(CloudwatchQueryPassthrough.class);

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
        return Arrays.asList(ENDTIME, QUERYSTRING, STARTTIME, LOGGROUPNAME);
//        return Arrays.asList(ENDTIME, LIMIT, LOGGROUPIDENTIFIERS, LOGGROUPNAME, LOGGROUPNAMES, QUERYSTRING, STARTTIME);
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }

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
//            if (!engineQptArguments.containsKey(argument)) {
//                throw new IllegalArgumentException("Missing Query Passthrough Argument: " + argument);
//            }
//            if (StringUtils.isEmpty(engineQptArguments.get(argument))) {
//                throw new IllegalArgumentException("Missing Query Passthrough Value for Argument: " + argument);
//            }
        }
        //Finally, perform any connector-specific verification;
        customConnectorVerifications(engineQptArguments);
    }
}
