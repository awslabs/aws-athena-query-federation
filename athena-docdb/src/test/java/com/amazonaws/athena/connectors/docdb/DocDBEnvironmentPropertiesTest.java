/*-
 * #%L
 * athena-mongodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.docdb;

import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_DOCDB;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DocDBEnvironmentPropertiesTest
{
    @Test
    public void connectionPropertiesToEnvironmentTest()
            throws IOException
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(PORT, "1234");
        connectionProperties.put(JDBC_PARAMS, "key=value&key2=value2");
        connectionProperties.put(SECRET_NAME, "secret");
        String connectionString = "mongodb://${secret}@localhost:1234/?key=value&key2=value2";

        Map<String, String> docdbConnectionProperties = new DocDBEnvironmentProperties().connectionPropertiesToEnvironment(connectionProperties);
        assertTrue(docdbConnectionProperties.containsKey(DEFAULT_DOCDB));
        assertEquals(connectionString, docdbConnectionProperties.get(DEFAULT_DOCDB));
    }

    @Test
    public void noJdbcParamsConnectionProperties()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(PORT, "1234");
        connectionProperties.put(SECRET_NAME, "secret");
        String connectionString = "mongodb://${secret}@localhost:1234";

        Map<String, String> docdbConnectionProperties = new DocDBEnvironmentProperties().connectionPropertiesToEnvironment(connectionProperties);
        assertTrue(docdbConnectionProperties.containsKey(DEFAULT_DOCDB));
        assertEquals(connectionString, docdbConnectionProperties.get(DEFAULT_DOCDB));
    }
}
