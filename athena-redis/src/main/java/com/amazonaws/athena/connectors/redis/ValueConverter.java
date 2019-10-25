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
