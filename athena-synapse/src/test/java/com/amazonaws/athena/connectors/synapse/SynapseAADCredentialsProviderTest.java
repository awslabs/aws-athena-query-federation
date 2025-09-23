/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import org.junit.Test;

import static com.amazonaws.athena.connector.credentials.CredentialsConstants.PASSWORD;
import static com.amazonaws.athena.connector.credentials.CredentialsConstants.USERNAME;
import static org.junit.Assert.assertEquals;

public class SynapseAADCredentialsProviderTest {
    private static final String TEST_USER = "testUser";
    private static final String TEST_PASSWORD = "testPassword";
    private static final String TEST_SECRET = String.format(
            "{\"%s\":\"%s\", \"%s\":\"%s\"}",
            USERNAME, TEST_USER,
            PASSWORD, TEST_PASSWORD
    );

    private static final String BASE_CONNECTION_STRING =
            "synapse://jdbc:sqlserver://testhost:1433;databaseName=testdb;";

    @Test
    public void testTransformSecretString_withActiveDirectoryServicePrincipal() {
        SynapseAADCredentialsProvider provider = new SynapseAADCredentialsProvider(TEST_SECRET);
        String connectionString = BASE_CONNECTION_STRING
                + "authentication=ActiveDirectoryServicePrincipal;${secret_name}";
        String transformed = provider.transformSecretString(connectionString);

        assertEquals(
                BASE_CONNECTION_STRING
                        + "authentication=ActiveDirectoryServicePrincipal;"
                        + "AADSecurePrincipalId=" + TEST_USER
                        + ";AADSecurePrincipalSecret=" + TEST_PASSWORD,
                transformed
        );
    }

    @Test
    public void testTransformSecretString_withoutActiveDirectoryServicePrincipal() {
        SynapseAADCredentialsProvider provider = new SynapseAADCredentialsProvider(TEST_SECRET);
        String connectionString = BASE_CONNECTION_STRING + "${secret_name}";
        String transformed = provider.transformSecretString(connectionString);

        assertEquals(
                BASE_CONNECTION_STRING,
                transformed
        );
    }
}
