/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune.qpt;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough.QUERY;

/**
 * A Singleton class that implements QPT signature interface to define
 * the Neptune Gremlin (PropertyGraph Type) Query Passthrough Function's signature that will be used
 * to inform the engine how to define QPT Function for a Neptune connector
 */
public final class NeptuneGremlinQueryPassthrough implements QueryPassthroughSignature
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NeptuneGremlinQueryPassthrough.class);

    // Constant value representing the name of the query.
    public static final String NAME = "traverse";

    // Constant value representing the domain of the query.
    public static final String SCHEMA_NAME = "system";

    // List of arguments for the query, statically initialized as it always contains the same value.
    public static final String DATABASE = "DATABASE";
    public static final String COLLECTION = "COLLECTION";
    public static final String TRAVERSE = "TRAVERSE";
    public static final String COMPONENT_TYPE = "COMPONENTTYPE";

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
        return Arrays.asList(DATABASE, COLLECTION, TRAVERSE, COMPONENT_TYPE);
    }

    @Override
    public Logger getLogger()
    {
        return LOGGER;
    }

    @Override
    public void customConnectorVerifications(Map<String, String> engineQptArguments)
    {
        // Verify no mixed operations (SPARQL and Gremlin in same request)
        if (engineQptArguments.containsKey(QUERY)) {
            throw new AthenaConnectorException("Mixed operations not supported: Cannot use both SPARQL query and Gremlin traverse in the same request", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
        else if (!engineQptArguments.get(TRAVERSE).contains(Constants.GREMLIN_QUERY_SUPPORT_TYPE)) {
            throw new AthenaConnectorException("Unsupported gremlin query format: We are currently supporting only valueMap gremlin queries. " +
                    "Please make sure you are using valueMap gremlin query. " +
                    "Example for valueMap query is g.V().hasLabel(\\\"airport\\\").valueMap().limit(5)", ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        }
    }
}
