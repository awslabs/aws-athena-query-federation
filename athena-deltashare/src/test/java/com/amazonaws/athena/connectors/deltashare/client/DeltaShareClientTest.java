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
package com.amazonaws.athena.connectors.deltashare.client;

import com.amazonaws.athena.connectors.deltashare.TestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class DeltaShareClientTest extends TestBase
{
    private static final String TEST_ENDPOINT = "https://test-endpoint.com";
    private static final String TEST_TOKEN = "test-token";

    @Test(expected = IllegalArgumentException.class)
    public void testClientWithNullEndpoint()
    {
        new DeltaShareClient(null, TEST_TOKEN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientWithEmptyEndpoint()
    {
        new DeltaShareClient("", TEST_TOKEN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientWithNullToken()
    {
        new DeltaShareClient(TEST_ENDPOINT, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientWithEmptyToken()
    {
        new DeltaShareClient(TEST_ENDPOINT, "");
    }

    @Test
    public void testClientCreation()
    {
        DeltaShareClient testClient = new DeltaShareClient(TEST_ENDPOINT, TEST_TOKEN);
        assertNotNull("Client should be created successfully", testClient);
    }

    @Test
    public void testClientCreationWithEndpointSlash()
    {
        DeltaShareClient testClient = new DeltaShareClient(TEST_ENDPOINT + "/", TEST_TOKEN);
        assertNotNull("Client should be created successfully with trailing slash", testClient);
    }

    @Test
    public void testClientCreationWithMockHttpClient()
    {
        DeltaShareClient testClient = new DeltaShareClient(TEST_ENDPOINT, TEST_TOKEN, null);
        assertNotNull("Client should be created successfully with null HttpClient", testClient);
    }
}