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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PROJECT_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryConstants.GCP_PROJECT_ID;

public class BigQueryEnvironmentProperties extends EnvironmentProperties
{
    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        Map<String, String> environment = new HashMap<>();

        if (connectionProperties.containsKey(PROJECT_ID)) {
            environment.put(GCP_PROJECT_ID, connectionProperties.get(PROJECT_ID));
        }
        environment.put(ENV_BIG_QUERY_CREDS_SM_ID, connectionProperties.get(SECRET_NAME));
        return environment;
    }
}
