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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;

import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HIVE_CONFS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HIVE_VARS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SESSION_CONFS;

public class ClouderaHiveEnvironmentProperties extends JdbcEnvironmentProperties
{
    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "hive://jdbc:hive2://";
    }

    @Override
    protected String getJdbcParameters(Map<String, String> connectionProperties)
    {
        StringBuilder params = new StringBuilder();

        // Add SESSION_CONFS if present
        if (connectionProperties.containsKey(SESSION_CONFS) && 
            !connectionProperties.get(SESSION_CONFS).trim().isEmpty()) {
            params.append(";").append(connectionProperties.get(SESSION_CONFS));
        }

        // Add HIVE_CONFS if present
        if (connectionProperties.containsKey(HIVE_CONFS) && 
            !connectionProperties.get(HIVE_CONFS).trim().isEmpty()) {
            params.append(";").append(connectionProperties.get(HIVE_CONFS));
        }

        // Add HIVE_VARS if present
        if (connectionProperties.containsKey(HIVE_VARS) && 
            !connectionProperties.get(HIVE_VARS).trim().isEmpty()) {
            params.append(";").append(connectionProperties.get(HIVE_VARS));
        }

        // Add SECRET_NAME if present
        if (connectionProperties.containsKey(SECRET_NAME) && 
            !connectionProperties.get(SECRET_NAME).trim().isEmpty()) {
            params.append(";").append("${").append(connectionProperties.get(SECRET_NAME)).append("}");
        }

        return params.toString();
    }
}
