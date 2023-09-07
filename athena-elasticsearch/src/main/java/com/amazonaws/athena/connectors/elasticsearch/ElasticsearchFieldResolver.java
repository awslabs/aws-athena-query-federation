/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used to resolve Elasticsearch complex structures to Apache Arrow Types.
 * @see com.amazonaws.athena.connector.lambda.data.FieldResolver
 */
public class ElasticsearchFieldResolver
        implements FieldResolver
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFieldResolver.class);

    protected ElasticsearchFieldResolver() {}

    /**
     * Return the field value from a complex structure or list.
     * @param field is the field that we would like to extract from the provided value.
     * @param originalValue is the original value object.
     * @return the field's value as a List for a LIST field type, a Map for a STRUCT field type, or the actual
     * value if neither of the above.
     * @throws IllegalArgumentException if originalValue is not an instance of Map.
     * @throws RuntimeException if the fieldName does not exist in originalValue, if the fieldType is a STRUCT and
     * the fieldValue is not instance of Map, or if the fieldType is neither a LIST or a STRUCT but the fieldValue
     * is instance of Map (STRUCT).
     */
    @Override
    public Object getFieldValue(Field field, Object originalValue)
            throws RuntimeException
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        String fieldName = field.getName();
        Object fieldValue;

        if (originalValue instanceof Map) {
            if (((Map) originalValue).containsKey(fieldName)) {
                fieldValue = ((Map) originalValue).get(fieldName);
            }
            else {
                // Ignore columns that do not exist in the document.
                return null;
            }
        }
        else {
            throw new IllegalArgumentException("Invalid argument type. Expecting a Map, but got: " +
                    originalValue.getClass().getTypeName());
        }

        switch (fieldType) {
            case LIST:
                return coerceListField(field, fieldValue);
            case STRUCT:
                if (fieldValue instanceof Map) {
                    // Both fieldType and fieldValue are nested structures => return as map.
                    return fieldValue;
                }
                break;
            default:
                return coerceField(field, fieldValue);
        }

        throw new RuntimeException("Invalid field value encountered in Document for field: " + field +
                ",value: " + fieldValue);
    }

    // Return the field value of a map key
    // For Elasticsearch, the key is always a string so we can return the originalValue
    @Override
    public Object getMapKey(Field field, Object originalValue)
    {
        return originalValue;
    }

    // Return the field value of a map value
    @Override
    public Object getMapValue(Field field, Object originalValue)
    {
        return coerceField(field, originalValue);
    }

    /**
     * Allows for coercion of a list of values where the returned types do not match the schema.
     * Multiple fields in Elasticsearch can be returned as a string, numeric (Integer, Long, Double), or null.
     * @param field is the field that we are coercing the value into.
     * @param fieldValue is the list of value to coerce
     * @return the coerced list of value.
     * @throws RuntimeException if the fieldType is not a LIST or the fieldValue is instanceof Map (STRUCT).
     */
    protected Object coerceListField(Field field, Object fieldValue)
            throws RuntimeException
    {
        if (fieldValue == null) {
            return null;
        }

        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        switch (fieldType) {
            case LIST:
                Field childField = field.getChildren().get(0);
                if (fieldValue instanceof List) {
                    // Both fieldType and fieldValue are lists => Return as a new list of values, applying coercion
                    // where necessary in order to match the type of the field being mapped into.
                    List<Object> coercedValues = new ArrayList<>();
                    ((List) fieldValue).forEach(value ->
                            coercedValues.add(coerceField(childField, value)));
                    return coercedValues;
                }
                else if (!(fieldValue instanceof Map)) {
                    // This is an abnormal case where the fieldType was defined as a list in the schema,
                    // however, the fieldValue returns as a single value => Return as a list of a single value
                    // applying coercion where necessary in order to match the type of the field being mapped into.
                    return Collections.singletonList(coerceField(childField, fieldValue));
                }
                break;
            default:
                break;
        }

        throw new RuntimeException("Invalid field value encountered in Document for field: " + field.toString() +
                ",value: " + fieldValue.toString());
    }

    /**
     * Allows for coercion of field-value types in the event that the returned type does not match the schema.
     * Multiple fields in Elasticsearch can be returned as a string, numeric (Integer, Long, Double), or null.
     * @param field is the field that we are coercing the value into.
     * @param fieldValue is the value to coerce
     * @return the coerced value.
     */
    private Object coerceField(Field field, Object fieldValue)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        if (fieldType != Types.MinorType.LIST && fieldValue instanceof List) {
            // This is an abnormal case where the field was not defined as a list in the schema,
            // however, fieldValue returns a list => return first item in list only applying coercion
            // where necessary in order to match the type of the field being mapped into.
            return coerceField(field, ((List) fieldValue).get(0));
        }

        switch (fieldType) {
            case LIST:
                return coerceListField(field, fieldValue);
            case BIGINT:
                if (!field.getMetadata().isEmpty() && field.getMetadata().containsKey("scaling_factor")) {
                    // scaled_float w/scaling_factor - a float represented as a long.
                    double scalingFactor = new Double(field.getMetadata().get("scaling_factor"));
                    if (fieldValue instanceof String) {
                        return Math.round(new Double((String) fieldValue) * scalingFactor);
                    }
                    else if (fieldValue instanceof Number) {
                        return Math.round(((Number) fieldValue).doubleValue() * scalingFactor);
                    }
                    break;
                }
                else if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).longValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).longValue();
                }
                break;
            case INT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).intValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).intValue();
                }
                break;
            case SMALLINT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).shortValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).shortValue();
                }
                break;
            case TINYINT:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue).byteValue();
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).byteValue();
                }
                break;
            case FLOAT8:
                if (fieldValue instanceof String) {
                    return new Double((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).doubleValue();
                }
                break;
            case FLOAT4:
                if (fieldValue instanceof String) {
                    return new Float((String) fieldValue);
                }
                else if (fieldValue instanceof Number) {
                    return ((Number) fieldValue).floatValue();
                }
                break;
            case DATEMILLI:
                if (fieldValue instanceof String) {
                    try {
                        return toEpochMillis((String) fieldValue);
                    }
                    catch (DateTimeParseException error) {
                        logger.warn("Error parsing localDateTime: {}.", error.getMessage());
                        return null;
                    }
                }
                if (fieldValue instanceof Number) {
                    // Date should be a long numeric value representing epoch milliseconds (e.g. 1589525370001).
                    return ((Number) fieldValue).longValue();
                }
                break;
            case BIT:
                if (fieldValue instanceof String) {
                    return new Boolean((String) fieldValue);
                }
                break;
            default:
                break;
        }

        return fieldValue;
    }

    /**
     * Converts a date-time string to epoch-milliseconds. The ISO_ZONED_DATE_TIME format will be attempted first,
     * followed by the ISO_LOCAL_DATE_TIME format if the previous one fails. Examples of formats that will work:
     * 1) "2020-05-18T10:15:30.123456789"
     * 2) "2020-05-15T06:50:01.123Z"
     * 3) "2020-05-15T06:49:30.123-05:00".
     * Nanoseconds will be rounded to the nearest millisecond.
     * @param dateTimeValue is the date-time value to be converted to epoch-milliseconds.
     * @return a long value representing the epoch-milliseconds derived from dateTimeValue.
     * @throws DateTimeParseException
     */
    private long toEpochMillis(String dateTimeValue)
            throws DateTimeParseException
    {
        long epochSeconds;
        double nanoSeconds;

        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateTimeValue,
                    DateTimeFormatter.ISO_ZONED_DATE_TIME.withResolverStyle(ResolverStyle.SMART));
            epochSeconds = zonedDateTime.toEpochSecond();
            nanoSeconds = zonedDateTime.getNano();
        }
        catch (DateTimeParseException error) {
            LocalDateTime localDateTime = LocalDateTime.parse(dateTimeValue,
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
                            .withResolverStyle(ResolverStyle.SMART));
            epochSeconds = localDateTime.toEpochSecond(ZoneOffset.UTC);
            nanoSeconds = localDateTime.getNano();
        }

        return epochSeconds * 1000 + Math.round(nanoSeconds / 1000000);
    }
}
