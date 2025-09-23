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

import com.amazonaws.athena.connector.credentials.CredentialsConstants;
import com.amazonaws.athena.connector.credentials.DefaultCredentialsProvider;

/**
 * Azure Active Directory Service Principal credentials provider for Synapse.
 */
public class SynapseAADCredentialsProvider extends DefaultCredentialsProvider
{
    public SynapseAADCredentialsProvider(String secretString)
    {
        super(secretString);
    }

    @Override
    public String transformSecretString(String connectionString)
    {
        String transformedConnectionString = super.transformSecretString(connectionString);

        // If using AAD Service Principal authentication, append AADSecurePrincipalId and AADSecurePrincipalSecret
        if (transformedConnectionString.contains("authentication=ActiveDirectoryServicePrincipal")) {
            transformedConnectionString = transformedConnectionString + String.format(
                "AADSecurePrincipalId=%s;AADSecurePrincipalSecret=%s",
                getCredential().getProperties().get(CredentialsConstants.USER),
                getCredential().getProperties().get(CredentialsConstants.PASSWORD)
            );
        }
        return transformedConnectionString;
    }
}
