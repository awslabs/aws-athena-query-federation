/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.types.ObjectId;

/**
 * Helper class with useful methods for type conversion and coercion.
 */
public class TypeUtils
{
    private TypeUtils() {}

    /**
     * Allows for coercing types in the event that schema has evolved or there were other data issues.
     *
     * @param field The field that we are coercing the value into.
     * @param origVal The value to coerce
     * @return The coerced value.
     * @note This method does only basic coercion today but will likely support more advanced
     * coercions in the future as a way of dealing with schema evolution.
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
