/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PROJECT_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryConstants.ENV_BIG_QUERY_CREDS_SM_ID;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryConstants.GCP_PROJECT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BigQueryEnvironmentPropertiesTest {

    private Map<String, String> connectionProperties;
    private BigQueryEnvironmentProperties bigQueryEnvironmentProperties;
    private static final String SECRET_NAME_VALUE = "bigQuery_Secret";

    @Before
    public void setup() {
        connectionProperties = new HashMap<>();
        connectionProperties.put(SECRET_NAME, SECRET_NAME_VALUE);
        bigQueryEnvironmentProperties = new BigQueryEnvironmentProperties();
    }

    @Test
    public void testConnectionPropertiesToEnvironment_When_ProjectId_NotPresent() {
        Map<String, String> result = bigQueryEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertNull(result.get(GCP_PROJECT_ID));
        assertEquals(SECRET_NAME_VALUE, result.get(ENV_BIG_QUERY_CREDS_SM_ID));
    }

    @Test
    public void testConnectionPropertiesToEnvironment_When_ProjectId_Present() {
        final String expectedValue = "testProject";
        connectionProperties.put(PROJECT_ID, expectedValue);
        Map<String, String> result = bigQueryEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);
        assertEquals(expectedValue, result.get(GCP_PROJECT_ID));
        assertEquals(SECRET_NAME_VALUE, result.get(ENV_BIG_QUERY_CREDS_SM_ID));
    }

}