package com.amazonaws.athena.connector.lambda.data;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

public interface FieldResolver
{
    /**
     * Basic FieldResolver capable of resolving nested (or single level) Lists and Structs
     * if the List values are iterable and the Structs values are represented
     * as Map<String, Object"=></String,>
     *
     * @note This This approach is relatively simple and convenient in terms of programming
     * interface but sacrifices some performance due to Object overhead vs. using
     * ApacheArrow directly. It is provided for basic usecases which don't have a high
     * row count and also as a way to teach by example. For better performance, provide
     * your own FieldResolver. And for even better performance, use ApacheArrow directly.
     */
    FieldResolver DEFAULT = new FieldResolver()
    {
        public Object getFieldValue(Field field, Object value)
        {
            Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
            if (minorType == Types.MinorType.LIST) {
                return ((List) value).iterator();
            }
            else if (value instanceof Map) {
                return ((Map<String, Object>) value).get(field.getName());
            }
            throw new RuntimeException("Expected LIST type but found " + minorType);
        }
    };

    Object getFieldValue(Field field, Object value);
}
