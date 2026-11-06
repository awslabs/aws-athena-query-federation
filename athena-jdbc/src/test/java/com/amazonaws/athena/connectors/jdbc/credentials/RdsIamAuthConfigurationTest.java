/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.credentials;

import org.junit.Test;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RdsIamAuthConfigurationTest
{
    private static final String HOSTNAME = "mydb.us-east-1.rds.amazonaws.com";
    private static final int PORT = 5432;
    private static final String USERNAME = "athena_iam";

    private static final RdsIamAuthConfiguration CONFIG = new RdsIamAuthConfiguration(
            HOSTNAME,
            PORT,
            USERNAME,
            Region.US_EAST_1);

    @Test
    public void getHostname_WhenConstructed_ReturnsHostname()
    {
        assertEquals(HOSTNAME, CONFIG.getHostname());
    }

    @Test
    public void getPort_WhenConstructed_ReturnsPort()
    {
        assertEquals(PORT, CONFIG.getPort());
    }

    @Test
    public void getUsername_WhenConstructed_ReturnsUsername()
    {
        assertEquals(USERNAME, CONFIG.getUsername());
    }

    @Test
    public void getRegion_WhenConstructed_ReturnsRegion()
    {
        assertEquals(Region.US_EAST_1, CONFIG.getRegion());
    }

    @Test
    public void equals_WhenSameValues_ReturnsTrue()
    {
        RdsIamAuthConfiguration other = new RdsIamAuthConfiguration(
                HOSTNAME,
                PORT,
                USERNAME,
                Region.US_EAST_1);

        assertEquals(CONFIG, other);
        assertEquals(CONFIG.hashCode(), other.hashCode());
    }

    @Test
    public void equals_WhenDifferentValues_ReturnsFalse()
    {
        RdsIamAuthConfiguration other = new RdsIamAuthConfiguration(
                "other.us-east-1.rds.amazonaws.com",
                PORT,
                USERNAME,
                Region.US_EAST_1);

        assertNotEquals(CONFIG, other);
        assertNotEquals(CONFIG.hashCode(), other.hashCode());
    }

    @Test
    public void equals_WhenComparedToNull_ReturnsFalse()
    {
        assertNotEquals(null, CONFIG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenHostnameBlank_ThrowsException()
    {
        new RdsIamAuthConfiguration("", PORT, USERNAME, Region.US_EAST_1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenPortNotPositive_ThrowsException()
    {
        new RdsIamAuthConfiguration(HOSTNAME, 0, USERNAME, Region.US_EAST_1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_WhenUsernameBlank_ThrowsException()
    {
        new RdsIamAuthConfiguration(HOSTNAME, PORT, "", Region.US_EAST_1);
    }

    @Test(expected = NullPointerException.class)
    public void constructor_WhenRegionNull_ThrowsException()
    {
        new RdsIamAuthConfiguration(HOSTNAME, PORT, USERNAME, null);
    }
}
