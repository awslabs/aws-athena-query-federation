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
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Map;

/**
 * Used to resolve DocDB complex structures to Apache Arrow Types.
 *
 * @see FieldResolver
 */
public class GcsFieldResolver
        implements FieldResolver
{
    protected static final FieldResolver DEFAULT_FIELD_RESOLVER = new GcsFieldResolver();

    private GcsFieldResolver() {}

    @SuppressWarnings("unchecked")
    @Override
    public Object getFieldValue(Field field, Object value)
    {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        if (minorType == Types.MinorType.LIST) {
            return TypeUtils.coerce(field, ((Map<String, Object>) value).get(field.getName()));
        }
        else if (value instanceof Map) {
            Object rawVal = ((Map<String, Object>) value).get(field.getName());
            return TypeUtils.coerce(field, rawVal);
        }
        throw new RuntimeException("Expected LIST or Document type but found " + minorType);
    }
}
