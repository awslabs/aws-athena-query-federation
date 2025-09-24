/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.client;

import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class DeltaShareClientTest
{
    private static final String TEST_ENDPOINT = "https://test.delta.io/delta-sharing/";
    private static final String TEST_TOKEN = "test-token";
    
    private DeltaShareClient client;
    
    @Before
    public void setUp()
    {
        client = new DeltaShareClient(TEST_ENDPOINT, TEST_TOKEN);
    }
    
    @Test
    public void testClientCreation()
    {
        assertNotNull(client);
        assertEquals(TEST_ENDPOINT, client.getEndpoint());
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testClientCreationWithNullEndpoint()
    {
        new DeltaShareClient(null, TEST_TOKEN);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testClientCreationWithNullToken()
    {
        new DeltaShareClient(TEST_ENDPOINT, null);
    }
    
    @Test
    public void testEndpointNormalization()
    {
        DeltaShareClient clientWithoutSlash = new DeltaShareClient("https://test.delta.io/delta-sharing", TEST_TOKEN);
        assertEquals("https://test.delta.io/delta-sharing/", clientWithoutSlash.getEndpoint());
        
        DeltaShareClient clientWithSlash = new DeltaShareClient("https://test.delta.io/delta-sharing/", TEST_TOKEN);
        assertEquals("https://test.delta.io/delta-sharing/", clientWithSlash.getEndpoint());
    }
    
    @Test
    public void testClientClose() throws IOException
    {
        client.close();
        client.close();
    }
}
