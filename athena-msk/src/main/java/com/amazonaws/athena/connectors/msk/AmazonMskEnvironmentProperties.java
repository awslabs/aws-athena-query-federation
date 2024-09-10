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

import com.amazonaws.athena.connector.lambda.EnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

public class AmazonMskEnvironmentProperties extends EnvironmentProperties
{
    private static final String AUTH_TYPE = "auth_type";
    private static final String CUSTOM_AUTH_TYPE = "CUSTOM_AUTH_TYPE";
    private static final String CERTIFICATES_S3_REFERENCE = "certificates_s3_reference";
    private static final String GLUE_CERTIFICATES_S3_REFERENCE = "CERTIFICATE_S3_REFERENCE";
    private static final String SECRETS_MANAGER_SECRET = "secrets_manager_secret";
    private static final String KAFKA_ENDPOINT = "kafka_endpoint";

    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        Map<String, String> environment = new HashMap<>();

        environment.put(AUTH_TYPE, connectionProperties.get(CUSTOM_AUTH_TYPE));
        environment.put(CERTIFICATES_S3_REFERENCE, connectionProperties.getOrDefault(GLUE_CERTIFICATES_S3_REFERENCE, ""));
        environment.put(SECRETS_MANAGER_SECRET, connectionProperties.getOrDefault(SECRET_NAME, ""));
        environment.put(KAFKA_ENDPOINT, connectionProperties.get("HOST") + ":" + connectionProperties.get("PORT"));
        return environment;
    }
}
