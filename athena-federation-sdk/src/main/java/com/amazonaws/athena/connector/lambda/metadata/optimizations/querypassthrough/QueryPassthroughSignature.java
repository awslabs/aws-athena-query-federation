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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface QueryPassthroughSignature
{
    static final Logger LOGGER = LoggerFactory.getLogger(QueryPassthroughSignature.class);

    public static final String SCHEMA_FUNCTION_NAME = "schemaFunctionName";
    /**
     *
     * @return QPT Function's Schema (also known as a domain or namepsace)
     */
    public abstract String getFunctionSchema();

    /**
     *
     * @return QPT Function's name
     */
    public abstract String getFunctionName();

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
    public abstract List<String> getFunctionArguments();

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
        //Final perform any connector-specific verification;
        customConnectorVerifications();
    }

    /**
     * Provides a mechanism to perform custom connector verification logic.
     */
    default void customConnectorVerifications()
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
            LOGGER.info("Found signature: {}", receivedSignature);
            return receivedSignature.equalsIgnoreCase(getFunctionSignature());
        }
        LOGGER.warn("No matching function signature found: {}", getFunctionSignature());
        return false;
    }

    /**
     * Creates a list of Optimization that includes the query passthrough definition
     * @return
     */
    default List<OptimizationSubType> getQueryPassthroughCapabilities()
    {
        List<OptimizationSubType> queryPassthroughDefinition = new ArrayList<>(3);
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_SCHEMA.withSchema(getFunctionSchema()));
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_NAME.withName(getFunctionName()));
        queryPassthroughDefinition.add(QueryPassthrough.QUERY_PASSTHROUGH_ARGUMENTS.withArguments(getFunctionArguments()));

        return queryPassthroughDefinition;
    }
}
