/*-
 * #%L
 * athena-db2-as400
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
package com.amazonaws.athena.connectors.db2as400;

public class Db2As400Constants
{
    public static final String NAME = "db2as400";
    public static final String DRIVER_CLASS = "com.ibm.as400.access.AS400JDBCDriver";
    public static final int DEFAULT_PORT = 446;

    public static final String QRY_TO_LIST_SCHEMAS = "SELECT SCHEMA_NAME as name FROM QSYS2.SYSSCHEMAS ORDER BY SCHEMA_NAME";
    public static final String QRY_TO_LIST_TABLES_AND_VIEWS = "SELECT TABLE_NAME as name FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME";

    static final String PARTITION_QUERY = "SELECT TABLE_PARTITION FROM QSYS2.SYSPARTITIONSTAT WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    static final String COLUMN_INFO_QUERY = "SELECT COLUMN_NAME, DATA_TYPE FROM QSYS2.SYSCOLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    private Db2As400Constants() {}
}
