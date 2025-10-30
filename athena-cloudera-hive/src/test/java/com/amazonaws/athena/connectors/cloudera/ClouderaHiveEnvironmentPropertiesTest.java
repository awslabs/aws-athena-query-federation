/*-
 * #%L
 * athena-cloudera-hive
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
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HIVE_CONFS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HIVE_VARS;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SESSION_CONFS;
import static org.junit.Assert.assertEquals;

public class ClouderaHiveEnvironmentPropertiesTest
{
    private static final String BASE_CONNECTION_STRING = "hive://jdbc:hive2://localhost:49100/default;";
    private static final String HIVE_EXECUTION_ENGINE_MR = "hive.execution.engine=mr";
    private static final String MAPREDUCE_QA = "mapreduce.job.queuename=qa";
    private static final String ENV_QA = "env=qa";
    private static final String MAPREDUCE_TESTING = "mapreduce.job.queuename=testing";
    private static final String ENV_TEST = "env=test";
    private static final String TEST_SECRET = "${testSecret}";
    
    Map<String, String> connectionProperties;
    ClouderaHiveEnvironmentProperties clouderaHiveEnvironmentProperties;

    @Before
    public void setUp()
    {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "localhost");
        connectionProperties.put(DATABASE, "default");
        connectionProperties.put(SECRET_NAME, "testSecret");
        connectionProperties.put(PORT, "49100");
        clouderaHiveEnvironmentProperties = new ClouderaHiveEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_WithValidProperties_ReturnsCorrectConnectionString()
    {
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = BASE_CONNECTION_STRING + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithSessionConfsOnly_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, HIVE_EXECUTION_ENGINE_MR);
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = BASE_CONNECTION_STRING + HIVE_EXECUTION_ENGINE_MR + ";" + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithSessionAndHiveConfs_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, HIVE_EXECUTION_ENGINE_MR);
        connectionProperties.put(HIVE_CONFS, MAPREDUCE_TESTING);
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = BASE_CONNECTION_STRING + HIVE_EXECUTION_ENGINE_MR + ";" + MAPREDUCE_TESTING + ";" + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithAllConfigs_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, HIVE_EXECUTION_ENGINE_MR);
        connectionProperties.put(HIVE_CONFS, MAPREDUCE_TESTING);
        connectionProperties.put(HIVE_VARS, ENV_TEST);
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = BASE_CONNECTION_STRING + HIVE_EXECUTION_ENGINE_MR + ";" + MAPREDUCE_TESTING + ";" + ENV_TEST + ";" + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithHiveVarsOnly_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(HIVE_VARS, ENV_TEST);
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = BASE_CONNECTION_STRING + ENV_TEST + ";" + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithHiveConfsAndVars_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(HIVE_CONFS, MAPREDUCE_QA);
        connectionProperties.put(HIVE_VARS, ENV_QA);
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        String expectedConnectionString = BASE_CONNECTION_STRING + MAPREDUCE_QA + ";" + ENV_QA + ";" + TEST_SECRET;
        assertEquals(expectedConnectionString, clouderaHiveConnectionProperties.get(DEFAULT));
    }
}
