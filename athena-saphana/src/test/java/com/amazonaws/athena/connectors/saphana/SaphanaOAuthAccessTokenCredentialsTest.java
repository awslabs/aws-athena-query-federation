/*-
 * #%L
 * athena-saphana
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
package com.amazonaws.athena.connectors.saphana;

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class SaphanaOAuthAccessTokenCredentialsTest
{
    private static final String TEST_ACCESS_TOKEN = "test-access-token";

    @Test
    public void constructor_withValidToken_createsCredentials()
    {
        SaphanaOAuthAccessTokenCredentials credentials = new SaphanaOAuthAccessTokenCredentials(TEST_ACCESS_TOKEN);
        assertEquals(TEST_ACCESS_TOKEN, credentials.getAccessToken());
        Map<String, String> properties = credentials.getProperties();
        assertEquals("", properties.get(CredentialsConstants.USER));
        assertEquals(TEST_ACCESS_TOKEN, properties.get(CredentialsConstants.PASSWORD));
    }

    @Test
    public void getProperties_returnsUnmodifiableMapWithPasswordProperty()
    {
        SaphanaOAuthAccessTokenCredentials credentials = new SaphanaOAuthAccessTokenCredentials(TEST_ACCESS_TOKEN);
        Map<String, String> properties = credentials.getProperties();
        assertNotNull("Properties map should not be null", properties);
        assertEquals(TEST_ACCESS_TOKEN, properties.get(CredentialsConstants.PASSWORD));
        assertThrows(UnsupportedOperationException.class, () -> properties.put("key", "value"));
    }

    @Test(expected = NullPointerException.class)
    public void constructor_withNullToken_throwsException()
    {
        new SaphanaOAuthAccessTokenCredentials(null);
    }

    @Test
    public void constructor_withBlankToken_throwsIllegalArgumentException()
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new SaphanaOAuthAccessTokenCredentials("   "));
        assertEquals("Access token must not be blank", thrown.getMessage());
    }

    @Test
    public void constructor_withEmptyToken_throwsIllegalArgumentException()
    {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new SaphanaOAuthAccessTokenCredentials(""));
        assertEquals("Access token must not be blank", thrown.getMessage());
    }
}
