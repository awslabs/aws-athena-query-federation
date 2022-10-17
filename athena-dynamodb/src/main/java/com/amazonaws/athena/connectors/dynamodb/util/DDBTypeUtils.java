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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.DateTimeFormatterUtil;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriter;
import com.amazonaws.athena.connector.lambda.data.writers.fieldwriters.FieldWriterFactory;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connectors.dynamodb.resolver.DynamoDBFieldResolver;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

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
        logger.debug("inferArrowField invoked for key {} of class {}", key,
                value != null ? value.getClass() : null);
        if (value == null) {
            return null;
        }

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
                logger.warn("Automatic schema inference encountered empty List or Set {}. Unable to determine element types. Falling back to VARCHAR representation", key);
                child = inferArrowField("", "");
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
            return child == null
                    ? null
                    : new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                                Collections.singletonList(child));
        }
        else if (value instanceof Map) {
            List<Field> children = new ArrayList<>();
            // keys are always Strings in DDB's case
            Map<String, Object> doc = (Map<String, Object>) value;
            for (String childKey : doc.keySet()) {
                Object childVal = doc.get(childKey);
                Field child = inferArrowField(childKey, childVal);
                if (child != null) {
                    children.add(child);
                }
            }

            // Athena requires Structs to have child types and not be empty
            if (children.isEmpty()) {
                logger.warn("Automatic schema inference encountered empty Map {}. Unable to determine element types. Falling back to VARCHAR representation", key);
                return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            }

            return new Field(key, FieldType.nullable(Types.MinorType.STRUCT.getType()), children);
        }

        String className = (value == null || value.getClass() == null) ? "null" : value.getClass().getName();
        throw new RuntimeException("Unknown type[" + className + "] for field[" + key + "]");
    }

    /**
     * Converts certain Arrow POJOs to Java POJOs to make downstream conversion easier.
     * This is called from GetSplits request. Since DDBRecordMetadata object is used for any custom override for
     * data manipulation/formatting, it does not apply to GetSplits request.
     *
     * @param columnName column name where the input object comes from
     * @param object the input object
     * @return the converted-to object if convertible, otherwise the original object
     */
    public static Object convertArrowTypeIfNecessary(String columnName, Object object)
    {
        return convertArrowTypeIfNecessary(columnName, object, new DDBRecordMetadata(null));
    }

    /**
     * Converts certain Arrow POJOs to Java POJOs to make downstream conversion easier.
     *
     * @param columnName column name where the input object comes from
     * @param object the input object
     * @param recordMetadata metadata object from glue table that contains customer specified information
     *                       such as desired default timezone, or datetime formats
     * @return the converted-to object if convertible, otherwise the original object
     */
    public static Object convertArrowTypeIfNecessary(String columnName, Object object, DDBRecordMetadata recordMetadata)
    {
        if (object instanceof Text) {
            return object.toString();
        }
        else if (object instanceof LocalDateTime) {
            String datetimeFormat = recordMetadata.getDateTimeFormat(columnName);
            ZoneId dtz = ZoneId.of(recordMetadata.getDefaultTimeZone().toString());
            if (datetimeFormat != null) {
                return ((LocalDateTime) object).atZone(dtz).format(DateTimeFormatter.ofPattern(datetimeFormat));
            }
            return ((LocalDateTime) object).atZone(dtz).toInstant().toEpochMilli();
        }
        return object;
    }

    /**
     * Converts from DynamoDB Attribute Type to Arrow type.
     *
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

    /**
     * Coerces the raw value from DynamoDB to normalized type
     * @param value raw value from DynamoDB
     * @param field Arrow field from table schema
     * @param fieldType Corresponding MinorType for field
     * @param recordMetadata DDBRecordMetadata object containing any metadata that is passed from schema metadata
     * @return coerced value to normalized type
     */
    public static Object coerceValueToExpectedType(Object value, Field field,
                                                   Types.MinorType fieldType, DDBRecordMetadata recordMetadata)
    {
        if (!recordMetadata.isContainsCoercibleType()) {
            return value;
        }
        if (DDBRecordMetadata.isDateTimeFieldType(fieldType) && (value instanceof String || value instanceof BigDecimal)) {
            String dateTimeFormat = recordMetadata.getDateTimeFormat(field.getName());
            if (value instanceof String && StringUtils.isEmpty(dateTimeFormat)) {
                logger.info("Date format not in cache for column {}. Trying to infer format...", field.getName());
                dateTimeFormat = DateTimeFormatterUtil.inferDateTimeFormat((String) value);
                if (StringUtils.isNotEmpty(dateTimeFormat)) {
                    logger.info("Adding datetime format {} for column {} to cache", dateTimeFormat, field.getName());
                    recordMetadata.setDateTimeFormat(field.getName(), dateTimeFormat);
                }
            }
            value = coerceDateTimeToExpectedType(value, fieldType, dateTimeFormat, recordMetadata.getDefaultTimeZone());
        }
        else if (value instanceof Number) {
            // Number conversion from any DDB Numeric type to the correct field numeric type (as specified in schema)
            value = DDBTypeUtils.coerceNumberToExpectedType((Number) value, fieldType);
        }
        return value;
    }

    /**
     * Converts a numeric value extracted from the DDB record to the correct type (Integer, Long, etc..) expected
     * by Arrow for the field type.
     * @param value is the number extracted from the DDB record.
     * @param fieldType is the Arrow MinorType.
     * @return the converted value.
     */
    private static Object coerceNumberToExpectedType(Number value, Types.MinorType fieldType)
    {
        switch (fieldType) {
            case INT:
                return value.intValue();
            case TINYINT:
                return value.byteValue();
            case SMALLINT:
                return value.shortValue();
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

    private static Object coerceDateTimeToExpectedType(Object value, Types.MinorType fieldType,
                                                      String customerConfiguredFormat, ZoneId defaultTimeZone)
    {
        try {
            if (value instanceof String) {
                switch (fieldType) {
                    case DATEMILLI:
                        return DateTimeFormatterUtil.stringToDateTime((String) value, customerConfiguredFormat, defaultTimeZone);
                    case TIMESTAMPMILLITZ:
                        return DateTimeFormatterUtil.stringToZonedDateTime((String) value, customerConfiguredFormat, defaultTimeZone);
                    case DATEDAY:
                        return DateTimeFormatterUtil.stringToLocalDate((String) value, customerConfiguredFormat, defaultTimeZone);
                    default:
                        return value;
                }
            }
            else if (value instanceof BigDecimal) {
                switch (fieldType) {
                    case DATEMILLI:
                        return DateTimeFormatterUtil.bigDecimalToLocalDateTime((BigDecimal) value, defaultTimeZone);
                    case DATEDAY:
                        return DateTimeFormatterUtil.bigDecimalToLocalDate((BigDecimal) value, defaultTimeZone);
                    default:
                        return value;
                }
            }
            return value;
        }
        catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            return value;
        }
    }

    /**
     * Converts a Set to a List, and coerces all list items into the correct type. If value is not a Collection, this
     * method will return a List containing a single item.
     * @param value is the Set/List of items.
     * @param field is the LIST field containing a list type in the child field.
     * @param recordMetadata contains metadata information.
     * @return a List of coerced values.
     * @throws RuntimeException when value is instance of Map since a List is expected.
     */
    public static List<Object> coerceListToExpectedType(Object value, Field field, DDBRecordMetadata recordMetadata)
            throws RuntimeException
    {
        if (value == null) {
            return null;
        }

        Field childField = field.getChildren().get(0);
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(childField.getType());

        if (!(value instanceof Collection)) {
            if (value instanceof Map) {
                throw new RuntimeException("Unexpected type (Map) encountered for: " + childField.getName());
            }
            return Collections.singletonList(coerceValueToExpectedType(value, childField, fieldType, recordMetadata));
        }

        List<Object> coercedList = new ArrayList<>();
        if (fieldType == Types.MinorType.LIST) {
            // Nested lists: array<array<...>, array<...>, ...>
            ((Collection<?>) value).forEach(list -> coercedList
                    .add(coerceListToExpectedType(list, childField, recordMetadata)));
        }
        else {
            ((Collection<?>) value).forEach(item -> coercedList
                    .add(coerceValueToExpectedType(item, childField, fieldType, recordMetadata)));
        }
        return coercedList;
    }

    private static Map<String, AttributeValue> contextAsMap(Object context, boolean caseInsensitive)
    {
        Map<String, AttributeValue> contextAsMap = (Map<String, AttributeValue>) context;
        if (!caseInsensitive) {
            return contextAsMap;
        }

        TreeMap<String, AttributeValue> caseInsensitiveMap = new TreeMap<String, AttributeValue>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(contextAsMap);
        return caseInsensitiveMap;
    }

    /**
     * Create the appropriate field extractor used for extracting field values from a DDB based on the field type.
     * @param field
     * @param recordMetadata
     * @return
     */
    public static Optional<Extractor> makeExtractor(Field field, DDBRecordMetadata recordMetadata, boolean caseInsensitive)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());

        switch (fieldType) {
            //With schema inference, we translate all number fields(int, double, float..etc) to Decimals. If glue enable, we route to factory default case.
            case DECIMAL:
                return Optional.of((DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    Object value = ItemUtils.toSimpleValue(contextAsMap(context, caseInsensitive).get(field.getName()));
                    if (value != null) {
                        dst.isSet = 1;
                        dst.value = (BigDecimal) value;
                    }
                    else {
                        dst.isSet = 0;
                    }
                });
            case VARBINARY:
                return Optional.of((VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                    Object value = ItemUtils.toSimpleValue(item.get(field.getName()));
                    value = DDBTypeUtils.coerceValueToExpectedType(value, field, fieldType, recordMetadata);

                    if (value != null) {
                        dst.isSet = 1;
                        dst.value = (byte[]) value;
                    }
                    else {
                        dst.isSet = 0;
                    }
                });
            case BIT:
                return Optional.of((BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    AttributeValue attributeValue = (contextAsMap(context, caseInsensitive)).get(field.getName());
                    if (attributeValue != null) {
                        dst.isSet = 1;
                        dst.value = attributeValue.getBOOL() ? 1 : 0;
                    }
                    else {
                        dst.isSet = 0;
                    }
                });
            default:
                return Optional.empty();
        }
    }

    /**
     * Since GeneratedRowWriter doesn't yet support complex types (STRUCT, LIST..etc) we use this to create our own
     * FieldWriters via a custom FieldWriterFactory.
     * @param field is used to determine which factory to generate based on the field type.
     * @param recordMetadata is used to retrieve metadata of ddb types
     * @param resolver is used to resolve it to proper type
     * @return
     */
    public static FieldWriterFactory makeFactory(Field field, DDBRecordMetadata recordMetadata, DynamoDBFieldResolver resolver, boolean caseInsensitive)
    {
        Types.MinorType fieldType = Types.getMinorTypeForArrowType(field.getType());
        switch (fieldType) {
            case LIST:
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) ->
                        {
                            Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                            Object value = ItemUtils.toSimpleValue(item.get(field.getName()));
                            List valueAsList = value != null ? DDBTypeUtils.coerceListToExpectedType(value, field, recordMetadata) : null;
                            BlockUtils.setComplexValue(vector, rowNum, resolver, valueAsList);

                            return true;
                        };
            case STRUCT:
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) ->
                        {
                            Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                            Object value = ItemUtils.toSimpleValue(item.get(field.getName()));
                            value = DDBTypeUtils.coerceValueToExpectedType(value, field, fieldType, recordMetadata);
                            BlockUtils.setComplexValue(vector, rowNum, resolver, value);
                            return true;
                        };
            case MAP:
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) ->
                        {
                            Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                            Object value = ItemUtils.toSimpleValue(item.get(field.getName()));
                            value = DDBTypeUtils.coerceValueToExpectedType(value, field, fieldType, recordMetadata);
                            BlockUtils.setComplexValue(vector, rowNum, resolver, value);
                            return true;
                        };
            default:
                //Below are using DDBTypeUtils.coerceValueToExpectedType to the correct type user defined from glue.
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) ->
                        {
                            Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                            Object value = ItemUtils.toSimpleValue(item.get(field.getName()));
                            value = DDBTypeUtils.coerceValueToExpectedType(value, field, fieldType, recordMetadata);
                            BlockUtils.setValue(vector, rowNum, value);
                            return true;
                        };
        }
    }
}
