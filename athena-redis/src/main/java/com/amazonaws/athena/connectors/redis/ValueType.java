/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines the supported value types that can be used to define a Redis table in Glue and thus mapped to rows.
 */
public enum ValueType
{
    /**
     * The value is a single, literal value which requires no interpretation before conversion.
     */
    LITERAL("literal"),
    /**
     * The value is actually a set of literal values and so we should treat the value as a list of rows, converting
     * each value independently.
     */
    ZSET("zset"),
    /**
     * The value is a single multi-column row and the values in the hash should be mapped to columns in the table but each
     * value is still 1 row.
     */
    HASH("hash");

    private static final Map<String, ValueType> TYPE_MAP = new HashMap<>();

    static {
        for (ValueType next : ValueType.values()) {
            TYPE_MAP.put(next.id, next);
        }
    }

    private String id;

    ValueType(String id)
    {
        this.id = id;
    }

    public String getId()
    {
        return id;
    }

    public static ValueType fromId(String id)
    {
        ValueType result = TYPE_MAP.get(id);
        if (result == null) {
            throw new IllegalArgumentException("Unknown ValueType for id: " + id);
        }

        return result;
    }
}
