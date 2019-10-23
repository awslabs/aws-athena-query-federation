package com.amazonaws.athena.connectors.docdb;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.types.ObjectId;

public class TypeUtils
{
    private TypeUtils() {}

    /**
     * Allows for coercing types in the event that schema has evolved or there were other data issues.
     *
     * @param field
     * @param origVal
     * @return The coerced value.
     */
    public static Object coerce(Field field, Object origVal)
    {

        if (origVal == null) {
            return origVal;
        }

        if (origVal instanceof ObjectId) {
            return origVal.toString();
        }

        ArrowType arrowType = field.getType();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(arrowType);

        switch (minorType) {
            case VARCHAR:
                if (origVal instanceof String) {
                    return origVal;
                }
                else {
                    return String.valueOf(origVal);
                }
            default:
                return origVal;
        }
    }
}
