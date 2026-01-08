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

        assertEquals("hive://jdbc:hive2://localhost:49100/default;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithSessionConfsOnly_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, "hive.execution.engine=mr");
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://localhost:49100/default;hive.execution.engine=mr;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithSessionAndHiveConfs_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, "hive.execution.engine=mr");
        connectionProperties.put(HIVE_CONFS, "mapreduce.job.queuename=testing");
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://localhost:49100/default;hive.execution.engine=mr;mapreduce.job.queuename=testing;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithAllConfigs_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(SESSION_CONFS, "hive.execution.engine=mr");
        connectionProperties.put(HIVE_CONFS, "mapreduce.job.queuename=testing");
        connectionProperties.put(HIVE_VARS, "env=test");
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://localhost:49100/default;hive.execution.engine=mr;mapreduce.job.queuename=testing;env=test;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithHiveVarsOnly_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(HIVE_VARS, "env=test");
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://localhost:49100/default;env=test;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }

    @Test
    public void connectionPropertiesToEnvironment_WithHiveConfsAndVars_ReturnsCorrectConnectionString()
    {
        connectionProperties.put(HIVE_CONFS, "mapreduce.job.queuename=qa");
        connectionProperties.put(HIVE_VARS, "env=qa");
        Map<String, String> clouderaHiveConnectionProperties = clouderaHiveEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://localhost:49100/default;mapreduce.job.queuename=qa;env=qa;${testSecret}", clouderaHiveConnectionProperties.get(DEFAULT));
    }
}
