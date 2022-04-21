
/*-
 * #%L
 * athena-saphana
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

package com.amazonaws.athena.connectors.saphana;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class SaphanaConstants
{
    public static final String SAPHANA_NAME = "saphana";
    public static final String SAPHANA_DRIVER_CLASS = "com.sap.db.jdbc.Driver";
    public static final int SAPHANA_DEFAULT_PORT = 1025;
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String ALL_PARTITIONS = "0";
    static final String BLOCK_PARTITION_COLUMN_NAME = "PART_ID";
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String CASE_UPPER = "upper";
    static final String CASE_LOWER = "lower";
    public static final String SAPHANA_QUOTE_CHARACTER = "\"";
    /**
     * partition query for saphana
     */
    static final String GET_PARTITIONS_QUERY = "SELECT DISTINCT PART_ID FROM SYS.TABLE_PARTITIONS " +
            "WHERE TABLE_NAME = ? AND SCHEMA_NAME = ? AND PART_ID IS NOT NULL";

    /**
     * Fix: Data type conversion and join errors
     * Schema name was added in where clause in column type query
     */
    static final String DATA_TYPE_QUERY_FOR_TABLE = "SELECT COLUMN_NAME, DATA_TYPE_NAME AS DATA_TYPE " +
            "FROM SYS.TABLE_COLUMNS WHERE TABLE_NAME =? AND SCHEMA_NAME = ?";

    static final String DATA_TYPE_QUERY_FOR_VIEW = "SELECT COLUMN_NAME, DATA_TYPE_NAME AS DATA_TYPE " +
            "FROM SYS.VIEW_COLUMNS WHERE VIEW_NAME = ? AND SCHEMA_NAME = ?";

    static final int MAX_SPLITS_PER_REQUEST = 1000_000;
    /**
     * view query for saphana
     */
    static final String VIEW_CHECK_QUERY = "SELECT * FROM VIEWS WHERE SCHEMA_NAME = ? AND VIEW_NAME = ?";

    static final String TO_WELL_KNOWN_TEXT_FUNCTION = ".ST_AsWKT()";

    private SaphanaConstants() {}
}
