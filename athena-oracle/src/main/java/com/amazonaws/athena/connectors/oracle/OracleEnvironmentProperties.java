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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;

import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.ENFORCE_SSL;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;

public class OracleEnvironmentProperties extends JdbcEnvironmentProperties
{
    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        String prefix = "oracle://jdbc:oracle:thin:";
        if (connectionProperties.containsKey(SECRET_NAME)) {
            prefix = prefix + "${" + connectionProperties.get(SECRET_NAME) + "}";
        }
        if (connectionProperties.containsKey(ENFORCE_SSL)) {
            prefix = prefix + "@tcps://";
        }
        else {
            prefix = prefix + "@//";
        }

        return prefix;
    }

    @Override
    protected String getDatabase(Map<String, String> connectionProperties)
    {
        return "/" + connectionProperties.get(DATABASE);
    }

    @Override
    protected String getJdbcParameters(Map<String, String> connectionProperties)
    {
        return "";
    }
}
