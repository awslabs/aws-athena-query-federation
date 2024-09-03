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

import java.util.HashMap;
import java.util.Map;

public abstract class JdbcEnvironmentProperties extends EnvironmentProperties
{
    protected static final String DEFAULT = "default";
    protected static final String JDBC_PARAMS = "JDBC_PARAMS";
    protected static final String DATABASE = "DATABASE";

    @Override
    public Map<String, String> connectionPropertiesToEnvironment(Map<String, String> connectionProperties)
    {
        HashMap<String, String> environment = new HashMap<>();

        // now construct jdbc string
        String connectionString = getConnectionStringPrefix(connectionProperties) + connectionProperties.get("HOST")
                + ":" + connectionProperties.get("PORT") + getConnectionStringSuffix(connectionProperties);

        environment.put(DEFAULT, connectionString);
        return environment;
    }

    protected abstract String getConnectionStringPrefix(Map<String, String> connectionProperties);

    protected String getConnectionStringSuffix(Map<String, String> connectionProperties)
    {
        String suffix = "/" + connectionProperties.get(DATABASE) + "?"
                + connectionProperties.getOrDefault(JDBC_PARAMS, "");

        if (connectionProperties.containsKey(SECRET_NAME)) {
            if (connectionProperties.containsKey(JDBC_PARAMS)) { // need to add delimiter
                suffix = suffix + "&${" + connectionProperties.get(SECRET_NAME) + "}";
            }
            else {
                suffix = suffix + "${" + connectionProperties.get(SECRET_NAME) + "}";
            }
        }

        return suffix;
    }
}
