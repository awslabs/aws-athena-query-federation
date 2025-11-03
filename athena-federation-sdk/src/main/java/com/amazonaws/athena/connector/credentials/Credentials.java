/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.credentials;

import java.util.Map;

/**
 * Represents a set of credentials required to authenticate and connect to a database.
 * Implementations may provide credentials in different forms, such as username/password or OAuth tokens.
 */
public interface Credentials
{
    /**
     * Gets the credential properties for database authentication.
     * Keys are property names (e.g., "username", "password", "accesToken"),
     * and values are the associated property values.
     *
     * @return a map of credential property names to values
     */
    Map<String, String> getProperties();
}
