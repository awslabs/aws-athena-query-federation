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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GlueConnectionUtils
{
    public static final String DEFAULT_GLUE_CONNECTION = "glue_connection";
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
        if (glueConnectionName != null) {
            HashMap<String, String> cachedConfig = connectionNameCache.get(glueConnectionName);
            if (cachedConfig == null) {
                try {
                    HashMap<String, HashMap<String, String>> athenaDriverPropertiesToMap = new HashMap<String, HashMap<String, String>>();

                    AWSGlue awsGlue = AWSGlueClientBuilder.standard().withClientConfiguration(new ClientConfiguration().withConnectionTimeout(CONNECT_TIMEOUT)).build();
                    GetConnectionResult glueConnection = awsGlue.getConnection(new GetConnectionRequest().withName(glueConnectionName));
                    Connection connection = glueConnection.getConnection();
                    String athenaDriverPropertiesAsString = connection.getConnectionProperties().get("AthenaDriverProperties");
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        athenaDriverPropertiesToMap = mapper.readValue(athenaDriverPropertiesAsString, new TypeReference<HashMap>(){});
                    }
                    catch (Exception err) {
                         logger.error("Error Parsing AthenaDriverProperties JSON to Map", err.toString());
                    }
                    String[] propertySubsets = {"federationSdkProperties", "driverProperties"};
                    for (String subset : propertySubsets) {
                        envConfig.putAll(athenaDriverPropertiesToMap.get(subset));
                    }
                    connectionNameCache.put(glueConnectionName, envConfig);
                }
                catch (Exception err) {
                    logger.error("Error thrown during fetching of {} connection properties. {}", glueConnectionName, err.toString());
                }
            }else{
                return cachedConfig;
            }
        }
        else {            
            logger.debug("No Glue Connection name was defined in Environment Variables.");
        }
        return envConfig;
    }
}
