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
}
