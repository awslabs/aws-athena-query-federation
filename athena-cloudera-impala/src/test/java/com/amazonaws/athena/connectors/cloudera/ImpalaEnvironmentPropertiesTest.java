/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

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

public class ImpalaEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    ImpalaEnvironmentProperties impalaEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(DATABASE, "default");
        connectionProperties.put(SECRET_NAME, "testSecret");
        connectionProperties.put(PORT, "49172");
        impalaEnvironmentProperties = new ImpalaEnvironmentProperties();
    }

    @Test
    public void testImpalaConnectionProperties()
    {
        Map<String, String> impalaConnectionProperties = impalaEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "impala://jdbc:impala://localhost:49172/default;${testSecret}";
        assertEquals(expectedConnectionString, impalaConnectionProperties.get(DEFAULT));
    }
}
