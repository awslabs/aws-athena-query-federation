/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

public class Db2Constants
{
    public static final String NAME = "dbtwo";
    public static final String DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver";
    public static final int DEFAULT_PORT = 50001;

    public static final String QRY_TO_LIST_SCHEMAS = "select schemaname as name " +
            "from syscat.schemata " +
            "where schemaname not like 'SYS%' " +
            "and  schemaname not IN ('SQLJ', 'NULLID') " +
            "order by name";
    public static final String QRY_TO_LIST_TABLES_AND_VIEWS = "select tabname as name " +
            "from syscat.tables " +
            "where type in ('T', 'U', 'V', 'W') and " +
            "tabschema = ? order by tabname";

    static final String PARTITION_QUERY = "SELECT DATAPARTITIONID FROM SYSCAT.DATAPARTITIONS WHERE TABSCHEMA = ? AND TABNAME = ? AND SEQNO > 0";
    static final String COLUMN_INFO_QUERY = "select colname, typename from syscat.columns where tabschema = ? AND tabname = ?";
    private Db2Constants() {}
}
