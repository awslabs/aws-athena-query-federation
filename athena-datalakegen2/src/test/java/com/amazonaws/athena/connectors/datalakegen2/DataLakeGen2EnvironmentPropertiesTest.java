/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

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

public class DataLakeGen2EnvironmentPropertiesTest
{
    private Map<String, String> connectionProperties;
    private DataLakeGen2EnvironmentProperties dataLakeGen2EnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "afq-connection-000.sql.azure.net");
        connectionProperties.put(PORT, "1433");
        connectionProperties.put(DATABASE, "testDB");
        connectionProperties.put(SECRET_NAME, "secret");

        dataLakeGen2EnvironmentProperties = new DataLakeGen2EnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_WithValidProperties_ReturnsExpectedConnectionString()
    {
        Map<String, String> dataLakeGen2ConnectionProperties = dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "datalakegentwo://jdbc:sqlserver://afq-connection-000.sql.azure.net:1433;databaseName=testDB;${secret}";
        assertEquals(expectedConnectionString, dataLakeGen2ConnectionProperties.get(DEFAULT));
    }

    @Test
    public void getDelimiter_WithDefaultConfiguration_ReturnsSemicolonDelimiter()
    {
        assertEquals(";", dataLakeGen2EnvironmentProperties.getDelimiter());
    }

    @Test
    public void getConnectionStringPrefix_WithValidProperties_ReturnsPrefix()
    {
        assertEquals("datalakegentwo://jdbc:sqlserver://", dataLakeGen2EnvironmentProperties.getConnectionStringPrefix(connectionProperties));
    }

    @Test
    public void getJdbcParametersSeparator_WithDefaultConfiguration_ReturnsSemicolonSeparator()
    {
        assertEquals(";", dataLakeGen2EnvironmentProperties.getJdbcParametersSeparator());
    }

    @Test(expected = NullPointerException.class)
    public void connectionPropertiesToEnvironment_WithNullProperties_ThrowsNullPointerException()
    {
        dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(null);
    }

    @Test
    public void connectionPropertiesToEnvironment_WithEmptyProperties_ReturnsConnectionStringWithNulls()
    {
        Map<String, String> sqlServerConnectionProperties = dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(new HashMap<>());

        String expectedConnectionString = "datalakegentwo://jdbc:sqlserver://null:null;databaseName=null;";
        assertEquals(expectedConnectionString, sqlServerConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithMissingHost_ReturnsConnectionStringWithNullHost()
    {
        connectionProperties.remove(HOST);
        Map<String, String> sqlServerConnectionProperties = dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "datalakegentwo://jdbc:sqlserver://null:1433;databaseName=testDB;${secret}";
        assertEquals(expectedConnectionString, sqlServerConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithMissingPort_ReturnsConnectionStringWithNullPort()
    {
        connectionProperties.remove(PORT);
        Map<String, String> sqlServerConnectionProperties = dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "datalakegentwo://jdbc:sqlserver://afq-connection-000.sql.azure.net:null;databaseName=testDB;${secret}";
        assertEquals(expectedConnectionString, sqlServerConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithMissingDatabase_ReturnsConnectionStringWithNullDatabase()
    {
        connectionProperties.remove(DATABASE);
        Map<String, String> sqlServerConnectionProperties = dataLakeGen2EnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "datalakegentwo://jdbc:sqlserver://afq-connection-000.sql.azure.net:1433;databaseName=null;${secret}";
        assertEquals(expectedConnectionString, sqlServerConnectionProperties.get(DEFAULT));
    }
}