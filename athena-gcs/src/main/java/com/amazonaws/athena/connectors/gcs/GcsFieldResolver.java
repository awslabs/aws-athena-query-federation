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

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Used to resolve complex structures to Apache Arrow Types.
 *
 * @see FieldResolver
 */
public class GcsFieldResolver
        implements FieldResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsFieldResolver.class);
    protected static final FieldResolver DEFAULT_FIELD_RESOLVER = new GcsFieldResolver();

    private GcsFieldResolver() {}

    @SuppressWarnings("unchecked")
    @Override
    public Object getFieldValue(Field field, Object value)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        if (minorType == Types.MinorType.LIST) {
            return coerce(field, ((Map<String, Object>) value).get(field.getName()));
        }
        else if (value instanceof Map) {
            Object rawVal = ((Map<String, Object>) value).get(field.getName());
            return coerce(field, rawVal);
        }
        throw new RuntimeException("Expected LIST or Document type but found " + minorType);
    }

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
