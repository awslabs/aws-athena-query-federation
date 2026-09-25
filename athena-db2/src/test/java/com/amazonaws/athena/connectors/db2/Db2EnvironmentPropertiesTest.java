/*-
 * #%L
 * athena-db2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.db2;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class Db2EnvironmentPropertiesTest
{
    private Map<String, String> connectionProperties;
    private Db2EnvironmentProperties db2Properties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(DATABASE, "SAMPLE");
        connectionProperties.put(SECRET_NAME, "testSecret");
        connectionProperties.put(PORT, "50002");
        db2Properties = new Db2EnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_ValidProperties_ShouldReturnExpectedConnectionString()
    {
        Map<String, String> db2ConnectionProperties = db2Properties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "dbtwo://jdbc:db2://localhost:50002/SAMPLE:${testSecret}";
        assertEquals(expectedConnectionString, db2ConnectionProperties.get(DEFAULT));
    }

    @Test
    public void getConnectionStringPrefix_ValidProperties_ShouldReturnPrefix()
    {
        assertEquals("dbtwo://jdbc:db2://", db2Properties.getConnectionStringPrefix(connectionProperties));
    }

    @Test
    public void getJdbcParametersSeparator_ShouldReturnColonSeparator()
    {
        assertEquals(":", db2Properties.getJdbcParametersSeparator());
    }

    @Test
    public void getDelimiter_ShouldReturnSemicolonDelimiter()
    {
        assertEquals(";", db2Properties.getDelimiter());
    }

    @Test(expected = NullPointerException.class)
    public void connectionPropertiesToEnvironment_NullProperties_ShouldThrowNullPointerException()
    {
        db2Properties.connectionPropertiesToEnvironment(null);
    }
}
