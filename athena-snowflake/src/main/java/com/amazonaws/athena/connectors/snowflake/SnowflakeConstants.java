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
     * This constant limits the number of records for each partition. The default set to 500000
     * We arrived at this number after performance testing with datasets of different size
     */
    public static final int PARTITION_RECORD_COUNT = 500000;
    /**
     * This constant limits the number of partitions. The default set to 50. A large number may cause a timeout issue.
     * We arrived at this number after performance testing with datasets of different size
     */
    public static final int MAX_PARTITION_COUNT = 50;

    private SnowflakeConstants() {}
}
