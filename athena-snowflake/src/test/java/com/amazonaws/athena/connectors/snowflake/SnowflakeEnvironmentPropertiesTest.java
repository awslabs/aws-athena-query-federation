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
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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

@RunWith(MockitoJUnitRunner.class)
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
    public void testConstructorWithS3ExportEnabled()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "true");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertTrue(envProps.isS3ExportEnabled());
    }

    @Test
    public void testConstructorWithS3ExportDisabled()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "false");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void testConstructorWithS3ExportNotSpecified()
    {
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void testConstructorWithInvalidS3ExportValue()
    {
        properties.put(SnowflakeEnvironmentProperties.ENABLE_S3_EXPORT, "invalid");
        envProps = new SnowflakeEnvironmentProperties(properties);
        assertFalse(envProps.isS3ExportEnabled());
    }

    @Test
    public void testConnectionPropertiesToEnvironment()
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
        assertTrue(environment.get(DEFAULT).contains(SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST + ":" + TEST_PORT));
    }

    @Test
    public void testConnectionPropertiesToEnvironmentWithoutPort()
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
        assertTrue(environment.get(DEFAULT).contains(SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST));
        assertFalse(environment.get(DEFAULT).contains(":" + TEST_PORT));
    }

    @Test
    public void testConnectionPropertiesToEnvironmentWithNullValues()
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
        assertTrue(environment.get(DEFAULT).contains(SNOWFLAKE_JDBC_SNOWFLAKE + TEST_HOST));
    }

    @Test
    public void testGetConnectionStringPrefix()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String prefix = envProps.getConnectionStringPrefix(properties);
        assertEquals(SNOWFLAKE_JDBC_SNOWFLAKE, prefix);
    }

    @Test
    public void testGetDatabase()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String database = envProps.getDatabase(properties);
        assertEquals("", database);
    }

    @Test
    public void testGetJdbcParametersSeparator()
    {
        envProps = new SnowflakeEnvironmentProperties(new HashMap<>());
        String separator = envProps.getJdbcParametersSeparator();
        assertEquals("&", separator);
    }

    @Test
    public void testGetSnowFlakeParameterWithGlueConnection()
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
    public void testGetSnowFlakeParameterWithoutGlueConnection()
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
    public void testGetSnowFlakeParameterWithoutSchema()
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
    public void testGetSnowFlakeParameterWithNullValues()
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
        assertEquals("\"null\"", parameters.get(DEFAULT_WAREHOUSE));
        assertEquals("\"null\"", parameters.get(DB));
        assertEquals("\"null\"", parameters.get(DEFAULT_SCHEMA));
    }
}
