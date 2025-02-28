package com.amazonaws.athena.connectors.postgresql;

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

public class PostGreSqlEnvironmentPropertiesTest
{
    Map<String, String> connectionProperties;
    PostGreSqlEnvironmentProperties postGreSqlEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "postgres.host");
        connectionProperties.put(PORT, "5432");
        connectionProperties.put(DATABASE, "default");
        connectionProperties.put(JDBC_PARAMS, "key=value&key2=value2");
        connectionProperties.put(SECRET_NAME, "secret");
        postGreSqlEnvironmentProperties = new PostGreSqlEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesWithJdbcParams()
    {
        String connectionString = "postgres://jdbc:postgresql://postgres.host:5432/default?key=value&key2=value2&${secret}";
        Map<String, String> pgsqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertTrue(pgsqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, pgsqlConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesWithoutJdbcParams()
    {
        Map<String, String> noJdbcParams = new HashMap<>(connectionProperties);
        noJdbcParams.remove(JDBC_PARAMS);
        String connectionString = "postgres://jdbc:postgresql://postgres.host:5432/default?${secret}";
        Map<String, String> pgsqlConnectionProperties = postGreSqlEnvironmentProperties.connectionPropertiesToEnvironment(noJdbcParams);
        assertTrue(pgsqlConnectionProperties.containsKey(DEFAULT));
        assertEquals(connectionString, pgsqlConnectionProperties.get(DEFAULT));
    }
}
