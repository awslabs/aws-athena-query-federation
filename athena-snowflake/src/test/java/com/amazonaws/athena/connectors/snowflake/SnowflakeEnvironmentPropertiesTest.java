package com.amazonaws.athena.connectors.snowflake;

/*-
 * #%L
 * athena-snowflake
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

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_GLUE_CONNECTION;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SCHEMA;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.WAREHOUSE;
import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SnowflakeEnvironmentPropertiesTest
{
    public static final String DEFAULT_WAREHOUSE = "warehouse";
    private static final String TEST_WAREHOUSE = "test_warehouse";
    private static final String TEST_DATABASE = "test_database";
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_HOST = "test_host";
    private static final String TEST_PORT = "443";
    private static final String TEST_USER = "test_user";
    public static final String SNOWFLAKE_JDBC_SNOWFLAKE = "snowflake://jdbc:snowflake://";
    public static final String DB = "db";
    public static final String DEFAULT_SCHEMA = "schema";
    private Map<String, String> properties;
    private SnowflakeEnvironmentProperties envProps;

    @Before
    public void setUp()
    {
        properties = new HashMap<>();
    }

    @Test
    public void constructor_WithS3ExportEnabled_ReturnsTrue()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "true");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertTrue(envProps.isS3ExportEnabled());
    }

    @Test
    public void constructor_WithS3ExportDisabled_ReturnsFalse()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "false");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void constructor_WithS3ExportNotSpecified_ReturnsFalse()
    {
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void constructor_WithInvalidS3ExportValue_ReturnsFalse()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "invalid");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void connectionPropertiesToEnvironment_WithAllProperties_ReturnsCorrectEnvironment()
    {
        properties.put(WAREHOUSE, TEST_WAREHOUSE);
        properties.put(DATABASE, TEST_DATABASE);
        properties.put(SCHEMA, TEST_SCHEMA);
        properties.put(HOST, TEST_HOST);
        properties.put(PORT, TEST_PORT);
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals(TEST_WAREHOUSE, environment.get(WAREHOUSE));
        assertEquals(TEST_DATABASE, environment.get(DATABASE));
        assertEquals(TEST_SCHEMA, environment.get(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST + ":" + TEST_PORT;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithoutPort_ReturnsEnvironmentWithoutPort()
    {
        properties.put(WAREHOUSE, TEST_WAREHOUSE);
        properties.put(DATABASE, TEST_DATABASE);
        properties.put(SCHEMA, TEST_SCHEMA);
        properties.put(HOST, TEST_HOST);
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals(TEST_WAREHOUSE, environment.get(WAREHOUSE));
        assertEquals(TEST_DATABASE, environment.get(DATABASE));
        assertEquals(TEST_SCHEMA, environment.get(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithInvalidPortValues_SkipsInvalidPorts()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        
        // Test with non-numeric port - should be skipped
        properties.put(WAREHOUSE, TEST_WAREHOUSE);
        properties.put(DATABASE, TEST_DATABASE);
        properties.put(SCHEMA, TEST_SCHEMA);
        properties.put(HOST, TEST_HOST);
        properties.put(PORT, "abc");
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with negative port - should be skipped (port must be > 0)
        properties.put(PORT, "-1");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with zero port - should be skipped (port must be > 0)
        properties.put(PORT, "0");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with empty string port - should be skipped
        properties.put(PORT, "");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with port containing special characters - should be skipped (not numeric)
        properties.put(PORT, "8080@test");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with port containing spaces - should be valid (trimmed to 8080)
        properties.put(PORT, " 8080 ");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST + ":8080";
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with valid port number
        properties.put(PORT, "443");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST + ":443";
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with maximum valid port
        properties.put(PORT, "65535");
        environment = envProps.connectionPropertiesToEnvironment(properties);
        assertNotNull(environment);
        expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST + ":65535";
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithNullValues_ExcludesNullProperties()
    {
        properties.put(WAREHOUSE, null);
        properties.put(DATABASE, null);
        properties.put(SCHEMA, null);
        properties.put(HOST, TEST_HOST);
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertFalse(environment.containsKey(WAREHOUSE));
        assertFalse(environment.containsKey(DATABASE));
        assertFalse(environment.containsKey(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void getConnectionStringPrefix_WithProperties_ReturnsSnowflakeJdbcPrefix()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String prefix = envProps.getConnectionStringPrefix(properties);
        assertEquals(SNOWFLAKE_JDBC_SNOWFLAKE, prefix);
    }

    @Test
    public void getDatabase_WithProperties_ReturnsEmptyString()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String database = envProps.getDatabase(properties);
        assertEquals("", database);
    }

    @Test
    public void getJdbcParametersSeparator_Always_ReturnsAmpersand()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String separator = envProps.getJdbcParametersSeparator();
        assertEquals("&", separator);
    }

    @Test
    public void getSnowFlakeParameter_WithGlueConnection_ReturnsAllParameters()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, TEST_WAREHOUSE);
        connectionProperties.put(DATABASE, TEST_DATABASE);
        connectionProperties.put(SCHEMA, TEST_SCHEMA);
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"" + TEST_WAREHOUSE + "\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"" + TEST_DATABASE + "\"", parameters.get(DB));
        assertEquals("\"" + TEST_SCHEMA + "\"", parameters.get(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithoutGlueConnection_ExcludesWarehouseDatabaseSchema()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(WAREHOUSE, TEST_WAREHOUSE);
        connectionProperties.put(DATABASE, TEST_DATABASE);
        connectionProperties.put(SCHEMA, TEST_SCHEMA);
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertFalse(parameters.containsKey(DEFAULT_WAREHOUSE));
        assertFalse(parameters.containsKey(DB));
        assertFalse(parameters.containsKey(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithoutSchema_ExcludesSchemaParameter()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, TEST_WAREHOUSE);
        connectionProperties.put(DATABASE, TEST_DATABASE);
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"" + TEST_WAREHOUSE + "\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"" + TEST_DATABASE + "\"", parameters.get(DB));
        assertFalse(parameters.containsKey(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithNullValues_ExcludesNullParameters()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, null);
        connectionProperties.put(DATABASE, null);
        connectionProperties.put(SCHEMA, null);
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        // Null values should not be added to parameters
        assertFalse(parameters.containsKey(DEFAULT_WAREHOUSE));
        assertFalse(parameters.containsKey(DB));
        assertFalse(parameters.containsKey(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithPartialNullValues_ExcludesOnlyNullParameters()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, TEST_WAREHOUSE);
        connectionProperties.put(DATABASE, null);
        connectionProperties.put(SCHEMA, TEST_SCHEMA);
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"" + TEST_WAREHOUSE + "\"", parameters.get(DEFAULT_WAREHOUSE));
        // Null database should not be added
        assertFalse(parameters.containsKey(DB));
        assertEquals("\"" + TEST_SCHEMA + "\"", parameters.get(DEFAULT_SCHEMA));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithEmptyHashMap_ReturnsEnvironmentWithNullHost()
    {
        Map<String, String> emptyProperties = new HashMap<>();
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(emptyProperties);
        
        assertNotNull(environment);
        assertFalse(environment.containsKey(WAREHOUSE));
        assertFalse(environment.containsKey(DATABASE));
        assertFalse(environment.containsKey(SCHEMA));
        // When HOST is null, it appends "null" to the connection string
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + "null";
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithEmptyStringValues_ReturnsEnvironmentWithEmptyStrings()
    {
        properties.put(WAREHOUSE, "");
        properties.put(DATABASE, "");
        properties.put(SCHEMA, "");
        properties.put(HOST, "");
        properties.put(PORT, "");
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals("", environment.get(WAREHOUSE));
        assertEquals("", environment.get(DATABASE));
        assertEquals("", environment.get(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithSpecialCharacters_ReturnsEnvironmentWithSpecialCharacters()
    {
        String specialHost = "test-host@example.com";
        String specialWarehouse = "warehouse#123";
        String specialDatabase = "database$test";
        String specialSchema = "schema%name";
        String specialPort = "8080";
        
        properties.put(WAREHOUSE, specialWarehouse);
        properties.put(DATABASE, specialDatabase);
        properties.put(SCHEMA, specialSchema);
        properties.put(HOST, specialHost);
        properties.put(PORT, specialPort);
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals(specialWarehouse, environment.get(WAREHOUSE));
        assertEquals(specialDatabase, environment.get(DATABASE));
        assertEquals(specialSchema, environment.get(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + specialHost + ":" + specialPort;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithCaseSensitivity_PreservesCase()
    {
        String upperWarehouse = "TEST_WAREHOUSE";
        String lowerWarehouse = "test_warehouse";
        String mixedDatabase = "TestDatabase";
        String upperSchema = "TEST_SCHEMA";
        String lowerSchema = "test_schema";
        
        // Test with uppercase warehouse
        properties.put(WAREHOUSE, upperWarehouse);
        properties.put(DATABASE, mixedDatabase);
        properties.put(SCHEMA, upperSchema);
        properties.put(HOST, TEST_HOST);
        
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        Map<String, String> environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals(upperWarehouse, environment.get(WAREHOUSE));
        assertEquals(mixedDatabase, environment.get(DATABASE));
        assertEquals(upperSchema, environment.get(SCHEMA));
        String expectedConnectionString = SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST;
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
        
        // Test with lowercase warehouse
        properties.put(WAREHOUSE, lowerWarehouse);
        properties.put(SCHEMA, lowerSchema);
        environment = envProps.connectionPropertiesToEnvironment(properties);
        
        assertNotNull(environment);
        assertEquals(lowerWarehouse, environment.get(WAREHOUSE));
        assertEquals(lowerSchema, environment.get(SCHEMA));
        assertEquals(expectedConnectionString, environment.get(DEFAULT));
    }

    @Test
    public void getSnowFlakeParameter_WithEmptyHashMap_ReturnsEmptyParameters()
    {
        Map<String, String> emptyBaseProperty = new HashMap<>();
        Map<String, String> emptyConnectionProperties = new HashMap<>();
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(emptyBaseProperty, emptyConnectionProperties);
        
        assertNotNull(parameters);
        assertTrue(parameters.isEmpty());
    }

    @Test
    public void getSnowFlakeParameter_WithEmptyStringValues_ReturnsQuotedEmptyStrings()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, "");
        connectionProperties.put(DATABASE, "");
        connectionProperties.put(SCHEMA, "");
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"\"", parameters.get(DB));
        assertEquals("\"\"", parameters.get(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithSpecialCharacters_ReturnsQuotedSpecialCharacters()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, "warehouse@123");
        connectionProperties.put(DATABASE, "database#test");
        connectionProperties.put(SCHEMA, "schema$name");
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"warehouse@123\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"database#test\"", parameters.get(DB));
        assertEquals("\"schema$name\"", parameters.get(DEFAULT_SCHEMA));
    }

    @Test
    public void getSnowFlakeParameter_WithCaseSensitivity_PreservesCase()
    {
        Map<String, String> baseProperty = new HashMap<>();
        baseProperty.put(USER, TEST_USER);
        
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DEFAULT_GLUE_CONNECTION, "true");
        connectionProperties.put(WAREHOUSE, "TEST_WAREHOUSE");
        connectionProperties.put(DATABASE, "TestDatabase");
        connectionProperties.put(SCHEMA, "test_schema");
        
        Map<String, String> parameters = SnowflakeEnvironmentProperties.getSnowFlakeParameter(baseProperty, connectionProperties);
        
        assertNotNull(parameters);
        assertEquals(TEST_USER, parameters.get(USER));
        assertEquals("\"TEST_WAREHOUSE\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"TestDatabase\"", parameters.get(DB));
        assertEquals("\"test_schema\"", parameters.get(DEFAULT_SCHEMA));
    }
}
