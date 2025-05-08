/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.ENFORCE_SSL;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;

public class OracleEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    OracleEnvironmentProperties oracleEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "test.oracle.com");
        connectionProperties.put(PORT, "1521");
        connectionProperties.put(DATABASE, "orcl");
        connectionProperties.put(SECRET_NAME, "oracle-secret");
        oracleEnvironmentProperties = new OracleEnvironmentProperties();
    }

    @Test
    public void testOracleConnectionString_withoutSSL()
    {
        Map<String, String> oracleConnectionProperties = oracleEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "oracle://jdbc:oracle:thin:${oracle-secret}@//test.oracle.com:1521/orcl";
        assertEquals(expectedConnectionString, oracleConnectionProperties.get(DEFAULT));
    }

    @Test
    public void testOracleConnectionString_withSSL()
    {
        connectionProperties.put(ENFORCE_SSL, "true");
        Map<String, String> oracleConnectionProperties = oracleEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = "oracle://jdbc:oracle:thin:${oracle-secret}@tcps://test.oracle.com:1521/orcl";
        assertEquals(expectedConnectionString, oracleConnectionProperties.get(DEFAULT));
    }
}
