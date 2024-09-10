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
package com.amazonaws.athena.connector.lambda.connection;

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

public class EnvironmentProperties
{
    public static final String DEFAULT_GLUE_CONNECTION = "glue_connection";
    private static final int CONNECT_TIMEOUT = 250;
    protected static final String SECRET_NAME = "secret_name";
    protected static final String SPILL_KMS_KEY_ID = "spill_kms_key_id";
    protected static final String KMS_KEY_ID = "kms_key_id";
    protected static final Logger logger = LoggerFactory.getLogger(EnvironmentProperties.class);

    public Map<String, String> createEnvironment() throws RuntimeException
    {
        HashMap<String, String> lambdaEnvironment = new HashMap<>(System.getenv());
        String glueConnectionName = lambdaEnvironment.get(DEFAULT_GLUE_CONNECTION);

        HashMap<String, String> connectionEnvironment = new HashMap<>();
        if (StringUtils.isNotBlank(glueConnectionName)) {
            Connection connection = getGlueConnection(glueConnectionName);
            Map<String, String> connectionProperties = connection.connectionPropertiesAsStrings();
            connectionProperties.putAll(authenticationConfigurationToMap(connection.authenticationConfiguration()));

            connectionEnvironment.putAll(connectionPropertiesToEnvironment(connectionProperties));
            connectionEnvironment.putAll(athenaPropertiesToEnvironment(connection.athenaProperties()));
        }

        connectionEnvironment.putAll(lambdaEnvironment); // Overwrite connection environment variables with lambda environment variables
        return connectionEnvironment;
    }

    public Connection getGlueConnection(String glueConnectionName) throws RuntimeException
    {
        try {
            GlueClient awsGlue = GlueClient.builder()
                    .endpointOverride(new URI("https://glue-gamma.us-west-2.amazonaws.com"))
                    .httpClientBuilder(ApacheHttpClient
                            .builder()
                            .connectionTimeout(Duration.ofMillis(CONNECT_TIMEOUT)))
                    .build();
            GetConnectionResponse glueConnection = awsGlue.getConnection(GetConnectionRequest.builder().name(glueConnectionName).build());
            logger.debug("Successfully retrieved connection {}", glueConnectionName);
            return glueConnection.connection();
        }
        catch (Exception err) {
            logger.error("Failed to retrieve connection: {}, and parse the connection properties!", glueConnectionName);
            throw new RuntimeException(err.toString());
        }
    }

    private Map<String, String> authenticationConfigurationToMap(AuthenticationConfiguration auth)
    {
        Map<String, String> authMap = new HashMap<>();

        if (StringUtils.isNotBlank(auth.secretArn())) {
            String[] splitArn = auth.secretArn().split(":");
            authMap.put(SECRET_NAME, splitArn[splitArn.length - 1]);
        }
        return authMap;
    }

    /**
     * Maps glue athena properties to environment properties like 'kms_key_id'
     *
     * @param athenaProperties contains athena specific properties
     * */
    public Map<String, String> athenaPropertiesToEnvironment(Map<String, String> athenaProperties)
    {
        if (athenaProperties.containsKey(SPILL_KMS_KEY_ID)) {
            String kmsKeyId = athenaProperties.remove(SPILL_KMS_KEY_ID);
            athenaProperties.put(KMS_KEY_ID, kmsKeyId);
        }
        return athenaProperties;
    }

    /**
     * Maps glue connection properties to environment properties like 'default' and 'secret_manager_gcp_creds_name'
     * Default behavior is to not populate environment with these properties
     *
     * @param connectionProperties contains secret_name and connection properties
     */
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        return new HashMap<>();
    }
}
