/*-
 * #%L
 * athena-cloudera-impala
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
package com.amazonaws.athena.connectors.cloudera;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ImpalaCredentialsProviderTest
{
    private static final String TEST_USERNAME = "testUser";
    private static final String TEST_PASSWORD = "testPassword";
    private static final String VALID_SECRET_STRING = "{\"username\": \"" + TEST_USERNAME + "\", \"password\": \"" + TEST_PASSWORD + "\"}";
    private static final String INVALID_SECRET_STRING = "invalid-json";

    private ImpalaCredentialsProvider credentialsProvider;

    @Test
    public void testGetCredentialMap()
    {
        credentialsProvider = new ImpalaCredentialsProvider(VALID_SECRET_STRING);
        Map<String, String> credentialMap = credentialsProvider.getCredentialMap();
        assertEquals("Should have exactly 2 entries", 2, credentialMap.size());
        assertEquals("Username should match", TEST_USERNAME, credentialMap.get(ImpalaConstants.USER));
        assertEquals("Password should match", TEST_PASSWORD, credentialMap.get(ImpalaConstants.PASSWORD));
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetCredentialMap_withInvalidSecret()
    {
        credentialsProvider = new ImpalaCredentialsProvider(INVALID_SECRET_STRING);
        credentialsProvider.getCredentialMap();
    }
}
