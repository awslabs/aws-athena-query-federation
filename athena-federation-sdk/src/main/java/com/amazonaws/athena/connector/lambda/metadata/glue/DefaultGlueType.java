package com.amazonaws.athena.connector.lambda.metadata.glue;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.HashMap;
import java.util.Map;

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

    private static Map<String, DefaultGlueType> TYPE_MAP = new HashMap<>();

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