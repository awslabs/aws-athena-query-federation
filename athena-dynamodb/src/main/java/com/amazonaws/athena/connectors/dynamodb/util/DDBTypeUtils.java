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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides utility methods relating to type handling.
 */
public final class DDBTypeUtils
{
    private static final Logger logger = LoggerFactory.getLogger(DDBTypeUtils.class);

    // DDB attribute "types"
    private static final String STRING = "S";
    private static final String NUMBER = "N";
    private static final String BOOLEAN = "BOOL";
    private static final String BINARY = "B";
    private static final String STRING_SET = "SS";
    private static final String NUMBER_SET = "NS";
    private static final String BINARY_SET = "BS";
    private static final String LIST = "L";
    private static final String MAP = "M";

    private DDBTypeUtils() {}

    /**
     * Infers an Arrow field from an object.  This has limitations when it comes to complex types such as Lists and Maps
     * and will fallback to VARCHAR fields in those cases.
     *
     * @param key the name of the field
     * @param value the value of the field
     * @return the inferred Arrow field
     */
    public static Field inferArrowField(String key, Object value)
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
            Field child = null;
            if (((Collection) value).isEmpty()) {
                try {
                    Object subVal = ((Collection) value).getClass()
                            .getTypeParameters()[0].getGenericDeclaration().newInstance();
                    child = inferArrowField(key + ".child", subVal);
                }
                catch (IllegalAccessException | InstantiationException ex) {
                    throw new RuntimeException(ex);
                }
            }
            else {
                Iterator iterator = ((Collection) value).iterator();
                Object firstValue = iterator.next();
                Class<?> aClass = firstValue.getClass();
                boolean allElementsAreSameType = true;
                while (iterator.hasNext()) {
                    if (!aClass.equals(iterator.next().getClass())) {
                        allElementsAreSameType = false;
                        break;
                    }
                }
                if (allElementsAreSameType) {
                    child = inferArrowField(key + ".element", firstValue);
                }
                else {
                    logger.warn("Automatic schema inference encountered List or Set {} containing multiple element types. Falling back to VARCHAR representation of elements", key);
                    child = inferArrowField("", "");
                }
            }
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                    Collections.singletonList(child));
        }
        else if (value instanceof Map) {
            List<Field> children = new ArrayList<>();
            // keys are always Strings in DDB's case
            Map<String, Object> doc = (Map<String, Object>) value;
            for (String childKey : doc.keySet()) {
                Object childVal = doc.get(childKey);
                Field child = inferArrowField(childKey, childVal);
                children.add(child);
            }

            // Athena requires Structs to have child types and not be empty
            if (children.isEmpty()) {
                logger.warn("Automatic schema inference encountered empty Map {}. Unable to determine element types. Falling back to VARCHAR representation", key);
                return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            }

            return new Field(key, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }

        String className = value.getClass() == null ? "null" : value.getClass().getName();
        throw new RuntimeException("Unknown type[" + className + "] for field[" + key + "]");
    }

    /**
     * Converts certain Arrow POJOs to Java POJOs to make downstream conversion easier.
     *
     * @param object the input object
     * @return the converted-to object if convertible, otherwise the original object
     */
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

    /**
     * Converts from DynamoDB Attribute Type to Arrow type.
     * @param attributeName the DDB Attribute name
     * @param attributeType the DDB Attribute type
     * @return the converted-to Arrow Field
     */
    public static Field getArrowFieldFromDDBType(String attributeName, String attributeType)
    {
        switch (attributeType) {
            case STRING:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            case NUMBER:
                return new Field(attributeName, FieldType.nullable(new ArrowType.Decimal(38, 9)), null);
            case BOOLEAN:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.BIT.getType()), null);
            case BINARY:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.VARBINARY.getType()), null);
            case STRING_SET:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null)));
            case NUMBER_SET:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("", FieldType.nullable(new ArrowType.Decimal(38, 9)), null)));
            case BINARY_SET:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("", FieldType.nullable(Types.MinorType.VARBINARY.getType()), null)));
            case LIST:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.LIST.getType()), null);
            case MAP:
                return new Field(attributeName, FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
            default:
                throw new RuntimeException("Unknown type[" + attributeType + "] for field[" + attributeName + "]");
        }
    }

    public static Object coerceDecimalToExpectedType(BigDecimal value, Types.MinorType fieldType)
    {
        switch (fieldType) {
            case INT:
            case TINYINT:
            case SMALLINT:
                return value.intValue();
            case BIGINT:
                return value.longValue();
            case FLOAT4:
                return value.floatValue();
            case FLOAT8:
                return value.doubleValue();
            default:
                return value;
        }
    }
}
