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
     * This constant limits the number of records to be returned in a single split.
     */
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

    private SnowflakeConstants() {}
}
