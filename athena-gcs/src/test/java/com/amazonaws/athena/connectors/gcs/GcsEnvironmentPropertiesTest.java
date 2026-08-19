/*-
 * #%L
 * athena-gcs
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
package com.amazonaws.athena.connectors.gcs;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.SECRET_NAME;
import static com.amazonaws.athena.connectors.gcs.GcsConstants.GCS_SECRET_KEY_ENV_VAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GcsEnvironmentPropertiesTest
{
    private GcsEnvironmentProperties gcsEnvironmentProperties;

    @Before
    public void setUp()
    {
        gcsEnvironmentProperties = new GcsEnvironmentProperties();
    }

    @Test
    public void connectionPropertiesToEnvironment_withValidSecretName_returnsSecretValue()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        final String expectedSecretValue = "my-secret-value";
        connectionProperties.put(SECRET_NAME, expectedSecretValue);

        Map<String, String> result = gcsEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        verifyAssertions(result);
        assertEquals("Value should match the secret name", expectedSecretValue, result.get(GCS_SECRET_KEY_ENV_VAR));
    }

    @Test
    public void connectionPropertiesToEnvironment_withoutSecretName_returnsNullSecretValue()
    {

        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put("other_key", "other_value");

        Map<String, String> result = gcsEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        verifyAssertions(result);
        assertNull("Value should be null when secret name is not present", result.get(GCS_SECRET_KEY_ENV_VAR));
    }

    @Test
    public void connectionPropertiesToEnvironment_withNullSecretValue_returnsNullSecretValue()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(SECRET_NAME, null);

        Map<String, String> result = gcsEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        verifyAssertions(result);
        assertNull("Value should be null when secret name value is null", result.get(GCS_SECRET_KEY_ENV_VAR));
    }

    @Test
    public void connectionPropertiesToEnvironment_withEmptySecretValue_returnsEmptySecretValue()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(SECRET_NAME, "");

        Map<String, String> result = gcsEnvironmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        verifyAssertions(result);
        assertEquals("Value should be empty string when secret name is empty", "", result.get(GCS_SECRET_KEY_ENV_VAR));
    }

    private void verifyAssertions(Map<String, String> result) {
        assertNotNull("Result should not be null", result);
        assertEquals("Result should contain one entry", 1, result.size());
        assertTrue("Result should contain GCS_SECRET_KEY_ENV_VAR", result.containsKey(GCS_SECRET_KEY_ENV_VAR));
    }
} 