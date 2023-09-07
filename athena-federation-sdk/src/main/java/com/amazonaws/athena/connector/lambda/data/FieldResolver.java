package com.amazonaws.athena.connector.lambda.data;

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
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;
import java.util.Map;

/**
 * Assists in writing values for complex types like List and Struct by providing a way to extract child field
 * values from the provided complex value.
 */
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
            if (value instanceof Map) {
                return ((Map<String, Object>) value).get(field.getName());
            }
            else if (minorType == Types.MinorType.LIST) {
                return ((List) value).iterator();
            }
            throw new RuntimeException("Expected LIST type but found " + minorType);
        }
    };

    /**
     * Used to extract a value for the given Field from the provided value.
     *
     * @param field The field that we would like to extract from the provided value.
     * @param value The complex value we'd like to extract the provided field from.
     * @return The value to use for the given field.
     */
    Object getFieldValue(Field field, Object value);

    /**
     * Allow for additional logic to be apply for the retrieval of map keys
     * If not overwritten, the default behavior is to assume a standard map with
     * no special logic where the key is NOT a complex value
     *
     * @param field The field that we would like to extract from the provided value.
     * @param value The complex value we'd like to extract the provided field from.
     * @return The value to use for the given field.
     */
    default Object getMapKey(Field field, Object value)
    {
        return value;
    }

    /**
     * Allow for additional logic to be apply for the retrieval of map values
     * If not overwritten, the default behavior is to assume a standard map with
     * no special logic and return the value directly
     *
     * @param field The field that we would like to extract from the provided value.
     * @param value The complex value we'd like to extract the provided field from.
     * @return The value to use for the given field.
     */
    default Object getMapValue(Field field, Object value)
    {
        return value;
    }
}
