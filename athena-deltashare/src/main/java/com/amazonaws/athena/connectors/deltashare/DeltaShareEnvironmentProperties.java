/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;
import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Environment properties handler for Delta Share connector.
 * Supports both direct environment variables and AWS Glue connection integration.
 */
public class DeltaShareEnvironmentProperties extends EnvironmentProperties
{
    private static final Logger logger = LoggerFactory.getLogger(DeltaShareEnvironmentProperties.class);

    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        Map<String, String> environment = new HashMap<>();
        
        if (connectionProperties.containsKey(DeltaShareConstants.ENDPOINT_PROPERTY)) {
            environment.put(DeltaShareConstants.ENDPOINT_PROPERTY, connectionProperties.get(DeltaShareConstants.ENDPOINT_PROPERTY));
        }
        
        if (connectionProperties.containsKey(DeltaShareConstants.TOKEN_PROPERTY)) {
            environment.put(DeltaShareConstants.TOKEN_PROPERTY, connectionProperties.get(DeltaShareConstants.TOKEN_PROPERTY));
        }
        
        if (connectionProperties.containsKey(DeltaShareConstants.SHARE_NAME_PROPERTY)) {
            environment.put(DeltaShareConstants.SHARE_NAME_PROPERTY, connectionProperties.get(DeltaShareConstants.SHARE_NAME_PROPERTY));
        }
        
        environment.putAll(connectionProperties);
        
        return environment;
    }
    
    @Override
    public Map<String, String> createEnvironment()
    {
        Map<String, String> environment = super.createEnvironment();
        
        String endpoint = environment.get(DeltaShareConstants.ENDPOINT_PROPERTY);
        if (endpoint == null || endpoint.trim().isEmpty()) {
            endpoint = System.getProperty(DeltaShareConstants.ENDPOINT_PROPERTY);
            if (endpoint != null && !endpoint.trim().isEmpty()) {
                environment.put(DeltaShareConstants.ENDPOINT_PROPERTY, endpoint);
            }
        }
        
        String token = environment.get(DeltaShareConstants.TOKEN_PROPERTY);
        if (token == null || token.trim().isEmpty()) {
            token = System.getProperty(DeltaShareConstants.TOKEN_PROPERTY);
            if (token != null && !token.trim().isEmpty()) {
                environment.put(DeltaShareConstants.TOKEN_PROPERTY, token);
            }
        }
        
        String shareName = environment.get(DeltaShareConstants.SHARE_NAME_PROPERTY);
        if (shareName == null || shareName.trim().isEmpty()) {
            shareName = System.getProperty(DeltaShareConstants.SHARE_NAME_PROPERTY);
            if (shareName != null && !shareName.trim().isEmpty()) {
                environment.put(DeltaShareConstants.SHARE_NAME_PROPERTY, shareName);
            }
        }
        
        return environment;
    }
    
}