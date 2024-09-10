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

import java.util.Map;

public class ClouderaHiveEnvironmentProperties extends JdbcEnvironmentProperties
{
    private static final String SESSION_CONFS = "SESSION_CONFS";
    private static final String HIVE_CONFS = "HIVE_CONFS";
    private static final String HIVE_VARS = "HIVE_VARS";

    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "hive://jdbc:hive2://";
    }

    @Override
    protected String getJdbcParameters(Map<String, String> connectionProperties)
    {
        String params = "?" + connectionProperties.getOrDefault(SESSION_CONFS, "");

        if (connectionProperties.containsKey(HIVE_CONFS)) {
            if (connectionProperties.containsKey(SESSION_CONFS)) {
                params = params + ";";
            }
            params = params + connectionProperties.get(HIVE_CONFS);
        }

        if (connectionProperties.containsKey(HIVE_VARS)) {
            if (connectionProperties.containsKey(HIVE_CONFS)) {
                params = params + ";";
            }
            params = params + connectionProperties.get(HIVE_VARS);
        }

        if (connectionProperties.containsKey(SECRET_NAME)) {
            if (connectionProperties.containsKey(HIVE_VARS)) { // need to add delimiter
                params = params + ";";
            }
            params = params + "${" + connectionProperties.get(SECRET_NAME) + "}";
        }

        return params;
    }
}
