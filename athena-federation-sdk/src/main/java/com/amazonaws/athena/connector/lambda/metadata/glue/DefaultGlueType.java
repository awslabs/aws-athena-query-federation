package com.amazonaws.athena.connector.lambda.metadata.glue;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines the default mapping of AWS Glue Data Catalog types to Apache Arrow types. You can override these by
 * overriding convertField(...) on GlueMetadataHandler.
 */
public enum DefaultGlueType
{
    INT("int", Types.MinorType.INT.getType()),
    VARCHAR("string", Types.MinorType.VARCHAR.getType()),
    BIGINT("bigint", Types.MinorType.BIGINT.getType()),
    DOUBLE("double", Types.MinorType.FLOAT8.getType()),
    FLOAT("float", Types.MinorType.FLOAT4.getType()),
    SMALLINT("smallint", Types.MinorType.SMALLINT.getType()),
    TINYINT("tinyint", Types.MinorType.TINYINT.getType()),
    BIT("boolean", Types.MinorType.BIT.getType()),
    VARBINARY("binary", Types.MinorType.VARBINARY.getType());

    private static final Map<String, DefaultGlueType> TYPE_MAP = new HashMap<>();

    static {
        for (DefaultGlueType next : DefaultGlueType.values()) {
            TYPE_MAP.put(next.id, next);
        }
    }

    private String id;
    private ArrowType arrowType;

    DefaultGlueType(String id, ArrowType arrowType)
    {
        this.id = id;
        this.arrowType = arrowType;
    }

    public static DefaultGlueType fromId(String id)
    {
        DefaultGlueType result = TYPE_MAP.get(id.toLowerCase());
        if (result == null) {
            throw new IllegalArgumentException("Unknown DefaultGlueType for id: " + id);
        }

        return result;
    }

    public static ArrowType toArrowType(String id)
    {
        DefaultGlueType result = TYPE_MAP.get(id.toLowerCase());
        if (result == null) {
            return null;
        }

        return result.getArrowType();
    }

    public ArrowType getArrowType()
    {
        return arrowType;
    }
}
