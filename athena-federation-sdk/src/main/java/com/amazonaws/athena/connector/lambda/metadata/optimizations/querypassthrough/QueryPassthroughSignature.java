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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface QueryPassthroughSignature
{
    public static final String SCHEMA_FUNCTION_NAME = "schemaFunctionName";
    public static final String ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    public static final String DEFAULT_ENABLE_QUERY_PASSTHROUGH_STATE = "true";
    /**
     *
     * @return QPT Function's Schema (also known as a domain or namepsace)
     */
    String getFunctionSchema();

    /**
     *
     * @return QPT Function's name
     */
    String getFunctionName();

    /**
     * Returns the full function schema and name
     * @return
     */
    default String getFunctionSignature()
    {
        return getFunctionSchema().toUpperCase() + "." + getFunctionName().toUpperCase();
    }

    /**
     *
     * @return Query Passthrough Function's Arguments
     */
    List<String> getFunctionArguments();

    /**
     * note: due to needing to stay compatible with JDK8; we can't use JDK9 private method
     * @return a logger
     */
    Logger getLogger();

    /**
     * verify that the arguments returned by the engine are the same that the connector defined
     * And call on any connector-custom verification
     * @param engineQptArguments
     * @throws IllegalArgumentException
     */
    public default void verify(Map<String, String> engineQptArguments)
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
            if (StringUtils.isEmpty(engineQptArguments.get(argument))) {
                throw new IllegalArgumentException("Missing Query Passthrough Value for Argument: " + argument);
            }
        }
        //Finally, perform any connector-specific verification;
        customConnectorVerifications(engineQptArguments);
    }

    /**
     * Provides a mechanism to perform custom connector verification logic.
     */
    default void customConnectorVerifications(Map<String, String> engineQptArguments)
    {
        //No Op
    }

    /**
     * Verifying that the query passthrough function signature is the one expected by the connector
     * @param argumentValues
     * @return true if the signature matches, otherwise false
     */
    default boolean verifyFunctionSignature(Map<String, String> argumentValues)
    {
        if (argumentValues.containsKey(SCHEMA_FUNCTION_NAME)) {
            String receivedSignature = argumentValues.get(SCHEMA_FUNCTION_NAME);
            getLogger().info("Found signature: {}", receivedSignature);
            return receivedSignature.equalsIgnoreCase(getFunctionSignature());
        }
        getLogger().warn("No matching function signature found: {}", getFunctionSignature());
        return false;
    }

    /**
     * Creates a list of Optimization that includes the query passthrough definition
     * @return list of capability describes of the signature of the current implementation
     */
    default List<OptimizationSubType> getQueryPassthroughCapabilities()
    {
        List<OptimizationSubType> queryPassthroughDefinition = new ArrayList<>(3);
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_SCHEMA.withSchema(getFunctionSchema()));
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_NAME.withName(getFunctionName()));
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_ARGUMENTS.withArguments(getFunctionArguments()));

        return queryPassthroughDefinition;
    }

    /**
     * Adds the query passthrough implementation, if user has not disabled it, to the connector's capabilities
     * @param capabilities
     * @param configOptions
     */
    default void addQueryPassthroughCapabilityIfEnabled(ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities, Map<String, String> configOptions)
    {
        if (allowQueryPassthrough(configOptions)) {
            getLogger().info("Query Passthrough is enabled; adding implementation to connector's capabilities");
            capabilities.put(getFunctionSignature(), getQueryPassthroughCapabilities());
        }
        else {
            getLogger().info("Query Passthrough is disabled");
        }
    }

    /**
     * A method that checks the Lambda's environment variables to see if QPT is disabled/enabled
     * @param configOptions
     * @return true if enabled; otherwise false
     */
    default boolean allowQueryPassthrough(Map<String, String> configOptions)
    {
        String enableQueryPassthroughEnvVal = configOptions
                .getOrDefault(ENABLE_QUERY_PASSTHROUGH, DEFAULT_ENABLE_QUERY_PASSTHROUGH_STATE)
                .toLowerCase();
        return enableQueryPassthroughEnvVal.equals("true");
    }
}
