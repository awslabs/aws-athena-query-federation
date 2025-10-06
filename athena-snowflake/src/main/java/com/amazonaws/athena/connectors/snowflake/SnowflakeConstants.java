/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

package com.amazonaws.athena.connectors.snowflake;

public final class SnowflakeConstants
{
    public static final String SNOWFLAKE_NAME = "snowflake";
    public static final String SNOWFLAKE_DRIVER_CLASS = "com.snowflake.client.jdbc.SnowflakeDriver";
    public static final int SNOWFLAKE_DEFAULT_PORT = 1025;
    /**
     * This constant limits the number of partitions. The default set to 50. A large number may cause a timeout issue.
     * We arrived at this number after performance testing with datasets of different size
     */
    public static final int MAX_PARTITION_COUNT = 50;
    /**
     * This constant limits the number of records to be returned in a single split.
     */
    public static final int SINGLE_SPLIT_LIMIT_COUNT = 10000;
    public static final String SNOWFLAKE_QUOTE_CHARACTER = "\"";
    /**
     * A ssl file location constant to store the SSL certificate
     * The file location is fixed at /tmp directory
     * to retrieve ssl certificate location
     */
    public static final String SSL_CERT_FILE_LOCATION = "SSL_CERT_FILE";
    public static final String SSL_CERT_FILE_LOCATION_VALUE = "/tmp/cacert.pem";
    public static final String SNOWFLAKE_SPLIT_QUERY_ID = "query_id";
    public static final String SNOWFLAKE_SPLIT_EXPORT_BUCKET = "exportBucket";
    public static final String SNOWFLAKE_SPLIT_OBJECT_KEY = "s3ObjectKey";
    /** Configuration key for specifying the authentication method */
    public static final String AUTHENTICATOR = "authenticator";

    /**
     * OAuth 2.0 Authentication Constants
     * These constants are used for configuring OAuth-based authentication with Snowflake.
     */
    public static final String AUTH_CODE = "auth_code";
    public static final String CLIENT_ID = "client_id";
    public static final String TOKEN_URL = "token_url";
    public static final String REDIRECT_URI = "redirect_uri";
    public static final String CLIENT_SECRET = "client_secret";

    /**
     * Key-Pair Authentication Constants
     * These constants are used for configuring public/private key pair authentication with Snowflake.
     */
    public static final String SF_USER = "sfUser";
    public static final String PEM_PRIVATE_KEY = "pem_private_key";
    public static final String PEM_PRIVATE_KEY_PASSPHRASE = "pem_private_key_passphrase";
    public static final String PRIVATE_KEY = "privateKey";

    /**
     * Password Authentication Constants
     * These constants are used for traditional username/password authentication with Snowflake.
     */
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String USER = "user";

    private SnowflakeConstants() {}
}
