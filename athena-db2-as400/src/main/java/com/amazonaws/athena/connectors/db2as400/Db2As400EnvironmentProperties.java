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
package com.amazonaws.athena.connectors.db2as400;

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;

public class Db2As400EnvironmentProperties extends EnvironmentProperties
{
    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        HashMap<String, String> environment = new HashMap<>();

        // now construct jdbc string
        String connectionString = "db2as400://jdbc:as400://" + connectionProperties.get(HOST)
                + ";" + connectionProperties.getOrDefault(JDBC_PARAMS, "");

        if (connectionProperties.containsKey(SECRET_NAME)) {
            if (connectionProperties.containsKey(JDBC_PARAMS)) { // need to add delimiter
                connectionString = connectionString + ";";
            }
            connectionString = connectionString + ":${" + connectionProperties.get(SECRET_NAME) + "}";
        }

        logger.debug("Constructed connection string: {}", connectionString);
        environment.put(DEFAULT, connectionString);
        return environment;
    }
}
