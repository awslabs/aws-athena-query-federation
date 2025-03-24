package com.amazonaws.athena.connectors.redshift;

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

public class RedshiftEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    RedshiftEnvironmentProperties redshiftEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "redshift.host");
        connectionProperties.put(PORT, "5439");
        connectionProperties.put(DATABASE, "default");
        connectionProperties.put(JDBC_PARAMS, "key=value&key2=value2");
        connectionProperties.put(SECRET_NAME, "secret");
        redshiftEnvironmentProperties = new RedshiftEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesWithJdbcParams()
    {
        String connectionString = "redshift://jdbc:redshift://redshift.host:5439/default?key=value&key2=value2&${secret}";
        Map<String, String> mysqlConnectionProperties = redshiftEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertTrue(mysqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, mysqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesWithoutJdbcParams()
    {
        Map<String, String> noJdbcParams = new HashMap<>(connectionProperties);
        noJdbcParams.remove(JDBC_PARAMS);
        String connectionString = "redshift://jdbc:redshift://redshift.host:5439/default?${secret}";
        Map<String, String> mysqlConnectionProperties = redshiftEnvironmentProperties.connectionPropertiesToEnvironment(noJdbcParams);
        assertTrue(mysqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, mysqlConnectionProperties.get(DEFAULT));
    }
}
