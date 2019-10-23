package com.amazonaws.athena.connectors.redis;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.UnsupportedEncodingException;

/**
 * Used to convert from Redis' native value/type system to the Apache Arrow type that was configured
 * for the particular field.
 */
public class ValueConverter
{
    private ValueConverter() {}

    /**
     * Allows for coercing types in the event that schema has evolved or there were other data issues.
     * @param field The Apache Arrow field that the value belongs to.
     * @param origVal The original value from Redis (before any conversion or coercion).
     * @return The coerced value.
     */
    public static Object convert(Field field, String origVal)
    {
        if (origVal == null) {
            return origVal;
        }

        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);

        switch (minorType) {
            case VARCHAR:
                return origVal;
            case INT:
            case SMALLINT:
            case TINYINT:
                return Integer.valueOf(origVal);
            case BIGINT:
                return Long.valueOf(origVal);
            case FLOAT8:
                return Double.valueOf(origVal);
            case FLOAT4:
                return Float.valueOf(origVal);
            case BIT:
                return Boolean.valueOf(origVal);
            case VARBINARY:
                try {
                    return origVal.getBytes("UTF-8");
                }
                catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            default:
                throw new RuntimeException("Unsupported type conversation " + minorType + " field: " + field.getName());
        }
    }
}
