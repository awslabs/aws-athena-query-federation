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

public enum SqlServerDataType {
    BIT(Types.MinorType.TINYINT.getType()),
    TINYINT(Types.MinorType.SMALLINT.getType()),
    NUMERIC(Types.MinorType.FLOAT8.getType()),
    SMALLMONEY(Types.MinorType.FLOAT8.getType()),
    DATE(Types.MinorType.DATEDAY.getType()),
    DATETIME(Types.MinorType.DATEMILLI.getType()),
    DATETIME2(Types.MinorType.DATEMILLI.getType()),
    SMALLDATETIME(Types.MinorType.DATEMILLI.getType()),
    DATETIMEOFFSET(Types.MinorType.DATEMILLI.getType());

    private ArrowType arrowType;

    SqlServerDataType(ArrowType arrowType)
    {
        this.arrowType = arrowType;
    }

    public static ArrowType fromType(String sqlServerType)
    {
        SqlServerDataType result = valueOf(sqlServerType.toUpperCase()); 
        return result.arrowType;
    }

    public static boolean isSupported(String dataType)
    {
        for (SqlServerDataType sqlServerDataType : values()) {
            if (sqlServerDataType.name().equalsIgnoreCase(dataType)) {
                return true;
            }
        }
        return false;
    }
}
