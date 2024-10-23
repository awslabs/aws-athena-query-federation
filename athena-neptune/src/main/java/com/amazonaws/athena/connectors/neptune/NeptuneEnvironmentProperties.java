/*-
 * #%L
 * Amazon Athena Query Federation SDK
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.CLUSTER_RES_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.GRAPH_TYPE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ClUSTER_RES_ID;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ENDPOINT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_GRAPH_TYPE;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_PORT;

public class NeptuneEnvironmentProperties extends EnvironmentProperties
{
    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        Map<String, String> environment = new HashMap<>();

        environment.put(CFG_ENDPOINT, connectionProperties.get(HOST));
        environment.put(CFG_PORT, connectionProperties.get(PORT));
        environment.put(CFG_ClUSTER_RES_ID, environment.get(CLUSTER_RES_ID));
        environment.put(CFG_GRAPH_TYPE, environment.get(GRAPH_TYPE));
        return environment;
    }
}
