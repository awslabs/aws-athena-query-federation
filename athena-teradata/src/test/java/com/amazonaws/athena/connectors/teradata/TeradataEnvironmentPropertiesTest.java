/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;

public class TeradataEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    TeradataEnvironmentProperties teradataEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "test.teradata.com");
        connectionProperties.put(DATABASE, "testdb");
        connectionProperties.put(SECRET_NAME, "testSecret");
        teradataEnvironmentProperties = new TeradataEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesWithCustomPort()
    {
        // adding custom port
        connectionProperties.put(PORT, "1234");

        Map<String, String> teradataConnectionProperties = teradataEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "teradata://jdbc:teradata://test.teradata.com/TMODE=ANSI,CHARSET=UTF8,DATABASE=testdb,DBS_PORT=1234,${testSecret}";
        assertEquals(expectedConnectionString, teradataConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesWithDefaultPort()
    {
        Map<String, String> teradataConnectionProperties = teradataEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "teradata://jdbc:teradata://test.teradata.com/TMODE=ANSI,CHARSET=UTF8,DATABASE=testdb,DBS_PORT=1025,${testSecret}";
        assertEquals(expectedConnectionString, teradataConnectionProperties.get(DEFAULT));
    }
}
