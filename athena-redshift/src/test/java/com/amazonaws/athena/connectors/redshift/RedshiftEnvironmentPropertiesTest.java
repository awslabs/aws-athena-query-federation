/*-
 * #%L
 * athena-redshift
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
package com.amazonaws.athena.connectors.redshift;

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

public class RedshiftEnvironmentPropertiesTest {
    Map<String, String> connectionProperties;
    RedshiftEnvironmentProperties redshiftEnvironmentProperties;

    @Before
    public void setUp() {
        connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "redshift-cluster-endpoint");
        connectionProperties.put(DATABASE, "testdb");
        connectionProperties.put(SECRET_NAME, "redshift-secret");
        connectionProperties.put(PORT, "5436");
        redshiftEnvironmentProperties = new RedshiftEnvironmentProperties();
    }

    @Test
    public void redshiftConnectionPropertiesTest() {

        Map<String, String> redshiftConnectionProperties = redshiftEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        String expectedConnectionString = "redshift://jdbc:redshift://redshift-cluster-endpoint:5436/testdb?${redshift-secret}";
        assertEquals(expectedConnectionString, redshiftConnectionProperties.get(DEFAULT));
    }
}
