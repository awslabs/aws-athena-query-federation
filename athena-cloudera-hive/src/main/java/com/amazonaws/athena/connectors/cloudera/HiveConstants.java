/*-
 * #%L
 * athena-cloudera-hive
 * %%
 * Copyright (C) 2019 - 2020 Amazon web services
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
package com.amazonaws.athena.connectors.cloudera;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class HiveConstants
{
    private HiveConstants()
    {
    }

    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String HIVE_QUOTE_CHARACTER = "";
    static final int FETCH_SIZE = 1000;
    static final String ALL_PARTITIONS = "*";
    public static final String HIVE_NAME = "hive";
    public static final String HIVE_DRIVER_CLASS = "com.cloudera.hive.jdbc.HS2Driver";
    public static final int HIVE_DEFAULT_PORT = 10000;
    public static final String METADATA_COLUMN_NAME = "col_name";
    public static final String METADATA_COLUMN_TYPE = "data_type";
}
