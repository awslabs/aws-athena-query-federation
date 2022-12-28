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
package com.amazonaws.athena.connectors.gcs;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class with useful methods for type conversion and coercion.
 */
public class TypeUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TypeUtils.class);

    private TypeUtils() {}

    /**
     * Allows for coercing types in the event that schema has evolved or there were other data issues.
     *
     * @param field The field that we are coercing the value into.
     * @param origVal The value to coerce
     * @return The coerced value.
     * coercions in the future as a way of dealing with schema evolution.
     */
    public static Object coerce(Field field, Object origVal)
    {
        if (origVal == null) {
            return origVal;
        }
        LOGGER.info("TypeUtils.coerce(Field, Object) :: FQCN is {}", origVal.getClass().getName());
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
            case FLOAT8:
                if (origVal instanceof Integer) {
                    return (double) (int) origVal;
                }
                else if (origVal instanceof Float) {
                    return (double) (float) origVal;
                }
                return origVal;
            case FLOAT4:
                if (origVal instanceof Integer) {
                    return (float) (int) origVal;
                }
                else if (origVal instanceof Double) {
                    return ((Double) origVal).floatValue();
                }
                return origVal;
            case INT:
                if (origVal instanceof Float) {
                    return ((Float) origVal).intValue();
                }
                else if (origVal instanceof Double) {
                    return ((Double) origVal).intValue();
                }
                return origVal;
            case DATEMILLI:
                return origVal.toString();
            default:
                return origVal;
        }
    }
}
