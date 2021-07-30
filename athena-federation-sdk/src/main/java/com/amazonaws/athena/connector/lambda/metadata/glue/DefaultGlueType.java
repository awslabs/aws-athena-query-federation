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

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Defines the default mapping of AWS Glue Data Catalog types to Apache Arrow types. You can override these by
 * overriding convertField(...) on GlueMetadataHandler.
 */
public enum DefaultGlueType
{
    INT("int", Types.MinorType.INT.getType()),
    VARCHAR("varchar", Types.MinorType.VARCHAR.getType()),
    STRING("string", Types.MinorType.VARCHAR.getType()),
    BIGINT("bigint", Types.MinorType.BIGINT.getType()),
    DOUBLE("double", Types.MinorType.FLOAT8.getType()),
    FLOAT("float", Types.MinorType.FLOAT4.getType()),
    SMALLINT("smallint", Types.MinorType.SMALLINT.getType()),
    TINYINT("tinyint", Types.MinorType.TINYINT.getType()),
    BIT("boolean", Types.MinorType.BIT.getType()),
    VARBINARY("binary", Types.MinorType.VARBINARY.getType()),
    TIMESTAMP("timestamp", Types.MinorType.DATEMILLI.getType()),
    // ZoneId.systemDefault().getId() is just a place holder, each row will have a TZ value
    // otherwise fall back to the table configured default TZ
    TIMESTAMPMILLITZ("timestamptz", new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, ZoneId.systemDefault().getId())),
    DATE("date", Types.MinorType.DATEDAY.getType());

    private static final Map<String, DefaultGlueType> TYPE_MAP = new HashMap<>();
    private static final Set<String> NON_COMPARABALE_SET = new HashSet<>();

    static {
        for (DefaultGlueType next : DefaultGlueType.values()) {
            TYPE_MAP.put(next.id, next);
        }

        NON_COMPARABALE_SET.add(DefaultGlueType.TIMESTAMPMILLITZ.name());
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

    public static Set<String> getNonComparableSet()
    {
        return NON_COMPARABALE_SET;
    }
}
