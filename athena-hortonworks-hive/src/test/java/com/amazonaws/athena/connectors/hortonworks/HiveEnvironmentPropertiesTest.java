/*-
 * #%L
 * athena-hortonworks-hive
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.hortonworks;

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

public class HiveEnvironmentPropertiesTest {
    private static final String TEST_HOST = "localhost";
    private static final String TEST_DATABASE = "default";
    private static final String TEST_PORT = "10000";
    private static final String TEST_SECRET = "testSecret";
    private static final String CONNECTION_STRING_PREFIX = "hive://jdbc:hive2://" + TEST_HOST + ":" + TEST_PORT + "/" + TEST_DATABASE;
    
    private Map<String, String> connectionProperties;
    private HortonworksEnvironmentProperties environmentProperties;
    
    @Before
    public void setUp() {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, TEST_HOST);
        connectionProperties.put(DATABASE, TEST_DATABASE);
        connectionProperties.put(PORT, TEST_PORT);
        environmentProperties = new HortonworksEnvironmentProperties();
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withSecret_returnsCorrectConnectionString() {
        connectionProperties.put(SECRET_NAME, TEST_SECRET);
        Map<String, String> result = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals(CONNECTION_STRING_PREFIX + "?${" + TEST_SECRET + "}", result.get(DEFAULT));
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withoutSecret_returnsCorrectConnectionString() {
        Map<String, String> result = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals(CONNECTION_STRING_PREFIX + "?", result.get(DEFAULT));
    }
    
    @Test(expected = NullPointerException.class)
    public void connectionPropertiesToEnvironment_withNullProperties_throwsNullPointerException() {
        environmentProperties.connectionPropertiesToEnvironment(null);
    }
    
    @Test
    public void connectionPropertiesToEnvironment_withMissingHost_returnsNullHostInConnectionString() {
        connectionProperties.remove(HOST);
        Map<String, String> result = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals("hive://jdbc:hive2://null:" + TEST_PORT + "/" + TEST_DATABASE + "?", result.get(DEFAULT));
    }
}
