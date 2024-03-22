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
package com.amazonaws.athena.connector.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.Connection;
import com.amazonaws.services.glue.model.GetConnectionRequest;
import com.amazonaws.services.glue.model.GetConnectionResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class GlueConnectionUtils
{
    // config property to store glue connection reference
    public static final String DEFAULT_GLUE_CONNECTION = "glue_connection";
    // Connection properties storing athena specific connection details
    public static final String GLUE_CONNECTION_ATHENA_PROPERTIES = "AthenaProperties";
    public static final String GLUE_CONNECTION_ATHENA_CONNECTOR_PROPERTIES = "connectorProperties";
    public static final String GLUE_CONNECTION_ATHENA_DRIVER_PROPERTIES = "driverProperties";
    public static final String[] propertySubsets = {GLUE_CONNECTION_ATHENA_CONNECTOR_PROPERTIES, GLUE_CONNECTION_ATHENA_DRIVER_PROPERTIES};

    private static final int CONNECT_TIMEOUT = 250;
    private static final Logger logger = LoggerFactory.getLogger(GlueConnectionUtils.class);
    private static HashMap<String, HashMap<String, String>> connectionNameCache = new HashMap<>();

    private GlueConnectionUtils()
    {
    }

    public static Map<String, String> getGlueConnection()
    {
        HashMap<String, String> envConfig = new HashMap<>(System.getenv());

        String glueConnectionName = envConfig.get(DEFAULT_GLUE_CONNECTION);
        if (StringUtils.isNotBlank(glueConnectionName)) {
            HashMap<String, String> cachedConfig = connectionNameCache.get(glueConnectionName);
            if (cachedConfig == null) {
                try {
                    HashMap<String, HashMap<String, String>> athenaPropertiesToMap = new HashMap<String, HashMap<String, String>>();

                    AWSGlue awsGlue = AWSGlueClientBuilder.standard().withClientConfiguration(new ClientConfiguration().withConnectionTimeout(CONNECT_TIMEOUT)).build();
                    GetConnectionResult glueConnection = awsGlue.getConnection(new GetConnectionRequest().withName(glueConnectionName));
                    logger.debug("Successfully retrieved connection {}", glueConnectionName);
                    Connection connection = glueConnection.getConnection();
                    String athenaPropertiesAsString = connection.getConnectionProperties().get(GLUE_CONNECTION_ATHENA_PROPERTIES);
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        athenaPropertiesToMap = mapper.readValue(athenaPropertiesAsString, new TypeReference<HashMap>(){});
                        logger.debug("Successfully parsed connection properties");
                    }
                    catch (Exception err) {
                         logger.error("Error Parsing AthenaDriverProperties JSON to Map", err.toString());
                    }
                    for (String subset : propertySubsets) {
                        if (athenaPropertiesToMap.containsKey(subset)) {
                            logger.debug("Adding {} subset from Glue Connection config.", subset);
                            Map<String, String> properties = athenaPropertiesToMap.get(subset).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, element -> String.valueOf(element.getValue())));
                            logger.debug("Adding the following set of properties to config: {}", properties);
                            envConfig.putAll(properties);
                        }
                        else {
                            logger.debug("{} properties not included in Glue Connnection config.", subset);
                        }
                    }
                    connectionNameCache.put(glueConnectionName, envConfig);
                }
                catch (Exception err) {
                    logger.error("Failed to retrieve connection: {}, and parse the connection properties!", glueConnectionName);
                    throw new RuntimeException(err.toString());
                }
            }
            else {
                return cachedConfig;
            }
        }
        else {            
            logger.debug("No Glue Connection name was defined in Environment Variables.");
        }
        return envConfig;
    }
}
