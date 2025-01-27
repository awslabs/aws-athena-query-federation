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
package com.amazonaws.athena.connectors.msk;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.CUSTOM_AUTH_TYPE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.GLUE_CERTIFICATES_S3_REFERENCE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.AUTH_TYPE;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.CERTIFICATES_S3_REFERENCE;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.ENV_KAFKA_ENDPOINT;
import static com.amazonaws.athena.connectors.msk.AmazonMskConstants.SECRET_MANAGER_MSK_CREDS_NAME;

public class AmazonMskEnvironmentProperties extends EnvironmentProperties
{
    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        Map<String, String> environment = new HashMap<>();

        environment.put(AUTH_TYPE, connectionProperties.get(CUSTOM_AUTH_TYPE));
        environment.put(CERTIFICATES_S3_REFERENCE, connectionProperties.getOrDefault(GLUE_CERTIFICATES_S3_REFERENCE, ""));
        environment.put(SECRET_MANAGER_MSK_CREDS_NAME, connectionProperties.getOrDefault(SECRET_NAME, ""));
        environment.put(ENV_KAFKA_ENDPOINT, connectionProperties.get(HOST) + ":" + connectionProperties.get(PORT));
        return environment;
    }
}
