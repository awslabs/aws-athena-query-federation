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

/**
 * Constants used for credentials and authentication.
 */
public final class CredentialsConstants
{
    private CredentialsConstants() {}

    // Constants for username/password authentication
    public static final String USERNAME = "username";
    public static final String USER = "user";
    public static final String PASSWORD = "password";

    // Constants for OAuth token response fields
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
    public static final String FETCHED_AT = "fetched_at";

    // Constants for OAuth configuration fields
    public static final String CLIENT_ID = "client_id";
    public static final String CLIENT_SECRET = "client_secret";

    // Property name used to set OAuth access token in JDBC connection properties
    public static final String ACCESS_TOKEN_PROPERTY = "accessToken";
}
