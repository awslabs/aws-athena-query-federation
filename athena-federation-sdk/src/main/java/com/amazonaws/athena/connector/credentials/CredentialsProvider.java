/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.credentials;

import java.util.Map;

/**
 * Provider interface for database credentials.
 */
public interface CredentialsProvider
{
    /**
     * Retrieves credentials for database connection.
     * @return Credentials object (username/password or OAuth)
     */
    Credentials getCredential();

    /**
     * Retrieves credential properties for database connection.
     * @return Map containing credential properties for database connection. The default implementation
     *         returns a map with "user" and "password" keys. Overriding implementations may return
     *         additional properties as needed for their specific authentication requirements.
     */
    default Map<String, String> getCredentialMap()
    {
        return getCredential().getProperties();
    }
}
