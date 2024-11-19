package com.amazonaws.athena.connectors.mysql;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DATABASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.JDBC_PARAMS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MySqlEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    MySqlEnvironmentProperties mySqlEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "mysql.host");
        connectionProperties.put(PORT, "3306");
        connectionProperties.put(DATABASE, "test");
        connectionProperties.put(JDBC_PARAMS, "key=value&key2=value2");
        connectionProperties.put(SECRET_NAME, "secret");
        mySqlEnvironmentProperties = new MySqlEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesWithJdbcParams()
    {
        String connectionString = "mysql://jdbc:mysql://mysql.host:3306/test?key=value&key2=value2&${secret}";
        Map<String, String> mysqlConnectionProperties = mySqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertTrue(mysqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, mysqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesWithoutJdbcParams()
    {
        Map<String, String> noJdbcParams = new HashMap<>(connectionProperties);
        noJdbcParams.remove(JDBC_PARAMS);
        String connectionString = "mysql://jdbc:mysql://mysql.host:3306/test?${secret}";
        Map<String, String> mysqlConnectionProperties = mySqlEnvironmentProperties.connectionPropertiesToEnvironment(noJdbcParams);
        assertTrue(mysqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, mysqlConnectionProperties.get(DEFAULT));
    }
}
