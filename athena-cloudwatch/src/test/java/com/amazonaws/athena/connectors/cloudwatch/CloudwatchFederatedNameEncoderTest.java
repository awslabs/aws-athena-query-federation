/*-
 * #%L
 * athena-cloudwatch
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Collections;

import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchFederatedNameEncoder.ENCODING_ENABLED_CONFIG_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CloudwatchFederatedNameEncoderTest
{
    private static final CloudwatchFederatedNameEncoder ENABLED =
            new CloudwatchFederatedNameEncoder(ImmutableMap.of(ENCODING_ENABLED_CONFIG_KEY, "true"));
    private static final CloudwatchFederatedNameEncoder DISABLED =
            new CloudwatchFederatedNameEncoder(Collections.emptyMap());

    // Disabled by default: every method is an identity function, so classic (non-managed) usage is unaffected.
    @Test
    public void disabledIsIdentity()
    {
        assertFalse(DISABLED.isEnabled());
        for (String name : new String[] {"/aws/lambda/MyFunc", "testcwmanaged", "all_log_streams", null}) {
            assertEquals(name, DISABLED.encode(name));
            assertEquals(name, DISABLED.decode(name));
        }
    }

    // Names the federation layer already accepts must be returned byte-for-byte unchanged, so existing
    // catalog entries and grants for legal-named log groups keep working exactly.
    @Test
    public void legalNamesUnchangedWhenEnabled()
    {
        assertTrue(ENABLED.isEnabled());
        for (String name : new String[] {"testcwmanaged", "all_log_streams", "my_service_logs",
                "my-log.group", "log123", "a.b-c_d#e"}) {
            assertEquals(name, ENABLED.encode(name));
            assertEquals(name, ENABLED.decode(name));
        }
    }

    // Illegal names round-trip, and the encoded form is itself catalog-legal.
    @Test
    public void illegalNamesRoundTripAndAreLegalWhenEncoded()
    {
        for (String name : new String[] {"/aws/lambda/MyFunction", "/aws/ecs/Cluster:1",
                "API-Gateway/Access_Logs", "UPPER", "with:colon", "/", ":"}) {
            String encoded = ENABLED.encode(name);
            assertNotEqualsCatalogIllegal(encoded);
            assertEquals(name, ENABLED.decode(encoded));
        }
    }

    // A (contrived) legal name that collides with the reserved prefix must still round-trip.
    @Test
    public void reservedPrefixCollisionRoundTrips()
    {
        String name = "__cwenc__foo";
        String encoded = ENABLED.encode(name);
        assertNotEqualsCatalogIllegal(encoded);
        assertEquals(name, ENABLED.decode(encoded));
    }

    private static void assertNotEqualsCatalogIllegal(String encoded)
    {
        for (int i = 0; i < encoded.length(); i++) {
            char c = encoded.charAt(i);
            assertFalse("encoded name must not contain '/'", c == '/');
            assertFalse("encoded name must not contain ':'", c == ':');
            assertFalse("encoded name must be lowercase", Character.isLetter(c) && Character.isUpperCase(c));
        }
    }
}
