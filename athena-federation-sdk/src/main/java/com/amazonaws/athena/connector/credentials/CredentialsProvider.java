/*-
 * #%L
 * athena-jdbc
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
 * JDBC username and password provider.
 */
public interface CredentialsProvider
{
    String USER = "user";
    String PASSWORD = "password";

    /**
     * Retrieves credential for database.
     *
     * @return JDBC credential. See {@link DefaultCredentials}.
     */
    DefaultCredentials getCredential();

    /**
     * Retrieves credential properties as a map for database connection.
     *
     * Default Behavior:
     * The default implementation returns a map containing only the basic "user" and "password" 
     * properties extracted from the {@link DefaultCredentials} object returned by {@link #getCredential()}.
     * This maintains backward compatibility with existing JDBC connection patterns.
     *
     * Extended Behavior:
     * Implementations can override this method to provide additional connection properties beyond 
     * just username and password. This enables support for advanced authentication mechanisms.
     *
     * Usage:
     * The returned map is directly applied to JDBC connection properties, allowing for seamless 
     * integration with various database drivers and authentication schemes without requiring 
     * custom connection factory implementations.
     *
     * @return Map containing credential properties for database connection. The default implementation
     *         returns a map with "user" and "password" keys. Overriding implementations may return
     *         additional properties as needed for their specific authentication requirements.
     */
    default Map<String, String> getCredentialMap()
    {
        DefaultCredentials credential = getCredential();
        return Map.of(USER, credential.getUser(), PASSWORD, credential.getPassword());
    }
}
