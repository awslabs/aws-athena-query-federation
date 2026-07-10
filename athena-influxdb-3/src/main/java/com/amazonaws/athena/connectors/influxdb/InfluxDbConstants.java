/*-
 * #%L
 * athena-influxdb
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.influxdb;

public final class InfluxDbConstants
{
    private InfluxDbConstants()
    {
    }

    public static final String SOURCE_TYPE = "influxdb";
    public static final String ENV_INFLUXDB_HOST = "INFLUXDB3_HOST_URL";
    public static final String ENV_INFLUXDB_TOKEN = "INFLUXDB3_AUTH_TOKEN";
    public static final String ENV_INFLUXDB_TOKEN_KEY = "INFLUXDB3_AUTH_TOKEN_KEY";
    public static final String DEFAULT_TOKEN_KEY = "token";
    public static final String ENABLE_QUERY_PARALLELISM = "enable_query_parallelism";
    public static final String QUERY_PARALLELISM_COUNT = "query_parallelism_count";
    public static final String TOKEN_REFRESH_MAX_RETRIES = "token_refresh_max_retries";
    public static final int DEFAULT_TOKEN_REFRESH_MAX_RETRIES = 3;
    public static final String PART_TIME_UPPER = "time_upper";
    public static final String PART_TIME_LOWER = "time_lower";
    public static final String DEFAULT_TIME_COLUMN = "time";
}
