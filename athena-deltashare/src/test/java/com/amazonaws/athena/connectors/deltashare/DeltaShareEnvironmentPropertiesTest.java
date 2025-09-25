/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare;

import com.amazonaws.athena.connectors.deltashare.constants.DeltaShareConstants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DeltaShareEnvironmentPropertiesTest
{
    private final DeltaShareEnvironmentProperties environmentProperties = new DeltaShareEnvironmentProperties();

    @Test
    public void testConnectionPropertiesToEnvironment()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DeltaShareConstants.ENDPOINT_PROPERTY, "https://sharing.delta.io/delta-sharing/");
        connectionProperties.put(DeltaShareConstants.TOKEN_PROPERTY, "dapi1234567890abcdef");
        connectionProperties.put(DeltaShareConstants.SHARE_NAME_PROPERTY, "test_share");
        connectionProperties.put("custom_property", "custom_value");

        Map<String, String> environment = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("https://sharing.delta.io/delta-sharing/", environment.get(DeltaShareConstants.ENDPOINT_PROPERTY));
        assertEquals("dapi1234567890abcdef", environment.get(DeltaShareConstants.TOKEN_PROPERTY));
        assertEquals("test_share", environment.get(DeltaShareConstants.SHARE_NAME_PROPERTY));
        assertEquals("custom_value", environment.get("custom_property"));
    }

    @Test
    public void testConnectionPropertiesToEnvironmentPartial()
    {
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(DeltaShareConstants.ENDPOINT_PROPERTY, "https://sharing.delta.io/delta-sharing/");
        connectionProperties.put("other_property", "other_value");

        Map<String, String> environment = environmentProperties.connectionPropertiesToEnvironment(connectionProperties);

        assertEquals("https://sharing.delta.io/delta-sharing/", environment.get(DeltaShareConstants.ENDPOINT_PROPERTY));
        assertNull(environment.get(DeltaShareConstants.TOKEN_PROPERTY));
        assertNull(environment.get(DeltaShareConstants.SHARE_NAME_PROPERTY));
        assertEquals("other_value", environment.get("other_property"));
    }


}