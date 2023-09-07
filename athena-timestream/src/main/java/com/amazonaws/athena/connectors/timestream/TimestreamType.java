/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import org.apache.arrow.vector.types.Types;

import java.util.HashMap;
import java.util.Map;

public enum TimestreamType
{
    VARCHAR("varchar", Types.MinorType.VARCHAR),
    DOUBLE("double", Types.MinorType.FLOAT8),
    BOOLEAN("boolean", Types.MinorType.BIT),
    TIMESTAMP("timestamp", Types.MinorType.DATEMILLI),
    BIGINT("bigint", Types.MinorType.BIGINT);

    private static final Map<String, TimestreamType> TIMESTREAM_TYPEMAP = new HashMap<>();

    static {
        for (TimestreamType next : values()) {
            TIMESTREAM_TYPEMAP.put(next.id, next);
        }
    }

    private String id;
    private Types.MinorType minorType;

    TimestreamType(String id, Types.MinorType minorType)
    {
        this.id = id;
        this.minorType = minorType;
    }

    public static TimestreamType fromId(String id)
    {
        TimestreamType result = TIMESTREAM_TYPEMAP.get(id);
        if (result == null) {
            throw new IllegalArgumentException("Unknown type for " + id);
        }
        return result;
    }

    public String getId()
    {
        return id;
    }

    public Types.MinorType getMinorType()
    {
        return minorType;
    }
}
