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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SCHEMA;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.WAREHOUSE;

public class SnowflakeEnvironmentProperties extends JdbcEnvironmentProperties
{
    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        HashMap<String, String> environment = new HashMap<>();

        // now construct jdbc string
        String connectionString = getConnectionStringPrefix(connectionProperties) + connectionProperties.get(HOST);
        if (connectionProperties.containsKey(PORT)) {
            connectionString = connectionString + ":" + connectionProperties.get(PORT);
        }
        connectionString = connectionString + getDatabase(connectionProperties) + getJdbcParameters(connectionProperties);

        environment.put(DEFAULT, connectionString);
        return environment;
    }

    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "snowflake://jdbc:snowflake://";
    }

    @Override
    protected String getDatabase(Map<String, String> connectionProperties)
    {
        if (!connectionProperties.containsKey(SCHEMA)) {
            logger.debug("No schema specified in connection string");
        }

        String databaseString = "/?warehouse=" + connectionProperties.get(WAREHOUSE)
                + "&db=" + connectionProperties.get(DATABASE)
                + "&schema=" + connectionProperties.get(SCHEMA);
        return databaseString;
    }

    @Override
    protected String getJdbcParametersSeparator()
    {
        return "&";
    }
}
