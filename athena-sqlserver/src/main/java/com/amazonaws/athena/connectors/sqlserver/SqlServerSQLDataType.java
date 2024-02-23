/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

public enum SqlServerSQLDataType {
    BIT("bit", Types.MinorType.TINYINT.getType()),
    TINYINT("tinyint", Types.MinorType.SMALLINT.getType()),
    NUMERIC("numeric", Types.MinorType.FLOAT8.getType()),
    SMALLMONEY("smallmoney", Types.MinorType.FLOAT8.getType()),
    DATE("date", Types.MinorType.DATEDAY.getType()),
    DATETIME("datetime", Types.MinorType.DATEMILLI.getType()),
    DATETIME2("datetime2", Types.MinorType.DATEMILLI.getType()),
    SMALLDATETIME("smalldatetime", Types.MinorType.DATEMILLI.getType()),
    DATETIMEOFFSET("datetimeoffset", Types.MinorType.DATEMILLI.getType());

    private static final Map<String, SqlServerSQLDataType> SQLSERVER_SQL_DATA_TYPE_MAP = new HashMap<>();

    static {
        for (SqlServerSQLDataType next : values()) {
            SQLSERVER_SQL_DATA_TYPE_MAP.put(next.sqlserverType, next);
        }
    }

    private String sqlserverType;
    private ArrowType arrowType;

    SqlServerSQLDataType(String sqlserverType, ArrowType arrowType)
    {
        this.sqlserverType = sqlserverType;
        this.arrowType = arrowType;
    }

    public static ArrowType fromType(String sqlserverType)
    {
        SqlServerSQLDataType result = SQLSERVER_SQL_DATA_TYPE_MAP.get(sqlserverType);
        return result.arrowType;
    }

    public static boolean isSupported(String datatype)
    {
        for (SqlServerSQLDataType sqlServerSQLDataType : values()) {
            if (sqlServerSQLDataType.name().equalsIgnoreCase(datatype)) {
                return true;
            }
        }
        return false;
    }
}
