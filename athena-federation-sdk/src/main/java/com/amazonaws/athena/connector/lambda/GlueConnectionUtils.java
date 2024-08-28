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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AuthenticationConfiguration;
import software.amazon.awssdk.services.glue.model.Connection;
import software.amazon.awssdk.services.glue.model.GetConnectionRequest;
import software.amazon.awssdk.services.glue.model.GetConnectionResponse;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class GlueConnectionUtils
{
    // config property to store glue connection reference
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
        if (StringUtils.isNotBlank(glueConnectionName)) {
            HashMap<String, String> cachedConfig = connectionNameCache.get(glueConnectionName);
            if (cachedConfig == null) {
                try {
                    GlueClient awsGlue = GlueClient.builder()
                            .endpointOverride(new URI("https://glue-gamma.ap-south-1.amazonaws.com"))
                            .httpClientBuilder(ApacheHttpClient
                                    .builder()
                                    .connectionTimeout(Duration.ofMillis(CONNECT_TIMEOUT)))
                            .build();
                    GetConnectionResponse glueConnection = awsGlue.getConnection(GetConnectionRequest.builder().name(glueConnectionName).build());
                    logger.debug("Successfully retrieved connection {}", glueConnectionName);
                    Connection connection = glueConnection.connection();
                    envConfig.putAll(connection.athenaProperties());
                    envConfig.putAll(connection.connectionPropertiesAsStrings());
                    envConfig.putAll(authenticationConfigurationToMap(connection.authenticationConfiguration()));
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

    private static Map<String, String> authenticationConfigurationToMap(AuthenticationConfiguration auth)
    {
        Map<String, String> authMap = new HashMap<>();

        String[] splitArn = auth.secretArn().split(":");
        authMap.put("secret_name", splitArn[splitArn.length - 1]);
        return authMap;
    }
}
