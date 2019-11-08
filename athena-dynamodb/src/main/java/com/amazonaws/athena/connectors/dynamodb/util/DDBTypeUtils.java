/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb.util;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class DDBTypeUtils
{
    private DDBTypeUtils() {}

    public static Field getArrowField(String key, Object value)
    {
        if (value instanceof String) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (value instanceof byte[]) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARBINARY.getType()), null);
        }
        else if (value instanceof Boolean) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIT.getType()), null);
        }
        else if (value instanceof BigDecimal) {
            return new Field(key, FieldType.nullable(new ArrowType.Decimal(38, 9)), null);
        }
        else if (value instanceof List || value instanceof Set) {
            Field child;
            if (((Collection) value).isEmpty()) {
                try {
                    Object subVal = ((Collection) value).getClass()
                            .getTypeParameters()[0].getGenericDeclaration().newInstance();
                    child = getArrowField("", subVal);
                }
                catch (IllegalAccessException | InstantiationException ex) {
                    throw new RuntimeException(ex);
                }
            }
            else {
                child = getArrowField("", ((Collection) value).iterator().next());
            }
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                    Collections.singletonList(child));
        }
        else if (value instanceof Map) {
            List<Field> children = new ArrayList<>();
            Map<String, Object> doc = (Map<String, Object>) value;
            for (String childKey : doc.keySet()) {
                Object childVal = doc.get(childKey);
                Field child = getArrowField(childKey, childVal);
                children.add(child);
            }
            return new Field(key, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }

        String className = value.getClass() == null ? "null" : value.getClass().getName();
        throw new RuntimeException("Unknown type[" + className + "] for field[" + key + "]");
    }

    public static Object convertArrowTypeIfNecessary(Object object)
    {
        if (object instanceof Text) {
            return object.toString();
        }
        else if (object instanceof LocalDateTime) {
            return ((LocalDateTime) object).toDateTime(DateTimeZone.UTC).getMillis();
        }
        return object;
    }
}
