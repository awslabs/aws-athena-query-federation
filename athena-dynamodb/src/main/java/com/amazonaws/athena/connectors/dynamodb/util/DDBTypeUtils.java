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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.BigDecimalAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.BooleanAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.ByteArrayAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.EnhancedAttributeValue;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.StringAttributeConverter;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.ImmutableMap;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
    public static Field inferArrowField(String key, AttributeValue value)
    {
        logger.debug("inferArrowField invoked for key {} of class {}", key,
                value != null ? value.toString() : null);

        EnhancedAttributeValue enhancedAttributeValue = EnhancedAttributeValue.fromAttributeValue(value);
        if (enhancedAttributeValue == null || enhancedAttributeValue.isNull()) {
            return null;
        }

        if (enhancedAttributeValue.isString()) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        }
        else if (enhancedAttributeValue.isBytes()) {
            return new Field(key, FieldType.nullable(Types.MinorType.VARBINARY.getType()), null);
        }
        else if (enhancedAttributeValue.isBoolean()) {
            return new Field(key, FieldType.nullable(Types.MinorType.BIT.getType()), null);
        }
        else if (enhancedAttributeValue.isNumber()) {
            return new Field(key, FieldType.nullable(new ArrowType.Decimal(38, 9)), null);
        }
        else if (enhancedAttributeValue.isSetOfBytes()) {
            Field child = new Field(key, FieldType.nullable(Types.MinorType.VARBINARY.getType()), null);
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),  Collections.singletonList(child));
        }
        else if (enhancedAttributeValue.isSetOfNumbers()) {
            Field child = new Field(key, FieldType.nullable(Types.MinorType.DECIMAL.getType()), null);
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),  Collections.singletonList(child));
        }
        else if (enhancedAttributeValue.isSetOfStrings()) {
            Field child = new Field(key, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            return new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()), Collections.singletonList(child));
        }
        else if (enhancedAttributeValue.isListOfAttributeValues()) { 
            Field child = null;
            List<AttributeValue> listOfAttributes = enhancedAttributeValue.asListOfAttributeValues();

            if (listOfAttributes.isEmpty()) {
                logger.warn("Automatic schema inference encountered empty List or Set {}. Unable to determine element types. Falling back to VARCHAR representation", key);
                child = inferArrowField("", EnhancedAttributeValue.nullValue().toAttributeValue());
            }
            else {
                Iterator<AttributeValue> iterator = listOfAttributes.iterator();
                EnhancedAttributeValue previousValue = EnhancedAttributeValue.fromAttributeValue(iterator.next());
                boolean allElementsAreSameType = true;
                while (iterator.hasNext()) {
                    EnhancedAttributeValue currentValue = EnhancedAttributeValue.fromAttributeValue(iterator.next());
                    // null is considered the same as any prior type
                    if (!previousValue.isNull() && !currentValue.isNull() && !previousValue.type().equals(currentValue.type())) {
                        allElementsAreSameType = false;
                        break;
                    }
                    // only update the previousValue if the currentValue is not null otherwise we will end
                    // up inferring the type as null if the currentValue is null and is the last value.
                    previousValue = (currentValue.isNull()) ? previousValue : currentValue;
                }
                if (allElementsAreSameType) {
                    child = inferArrowField(key + ".element", previousValue.toAttributeValue());
                }
                else {
                    logger.warn("Automatic schema inference encountered List or Set {} containing multiple element types. Falling back to VARCHAR representation of elements", key);
                    child = inferArrowField("", AttributeValue.builder().s("").build());
                }
            }
            return child == null
                    ? null
                    : new Field(key, FieldType.nullable(Types.MinorType.LIST.getType()),
                                Collections.singletonList(child));
        }
        else if (enhancedAttributeValue.isMap()) {
            List<Field> children = new ArrayList<>();
            // keys are always Strings in DDB's case
            Map<String, AttributeValue> doc = value.m();
            for (String childKey : doc.keySet()) {
                AttributeValue childVal = doc.get(childKey);
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

        String attributeTypeName = (value == null || value.getClass() == null) ? "null" : enhancedAttributeValue.type().name();
        throw new RuntimeException("Unknown Attribute Value Type[" + attributeTypeName + "] for field[" + key + "]");
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
                    case TIMESTAMPMICROTZ:
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
                    Object value = toSimpleValue(contextAsMap(context, caseInsensitive).get(field.getName()));
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
                    Object value = toSimpleValue(item.get(field.getName()));
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
                        dst.value = attributeValue.bool() ? 1 : 0;
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
                            Object value = toSimpleValue(item.get(field.getName()));
                            List valueAsList = value != null ? DDBTypeUtils.coerceListToExpectedType(value, field, recordMetadata) : null;
                            BlockUtils.setComplexValue(vector, rowNum, resolver, valueAsList);

                            return true;
                        };
            case STRUCT:
            case MAP:
                return (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
                        (FieldWriter) (Object context, int rowNum) ->
                        {
                            Map<String, AttributeValue> item = contextAsMap(context, caseInsensitive);
                            Object value = toSimpleValue(item.get(field.getName()));
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
                            Object value = toSimpleValue(item.get(field.getName()));
                            value = DDBTypeUtils.coerceValueToExpectedType(value, field, fieldType, recordMetadata);
                            BlockUtils.setValue(vector, rowNum, value);
                            return true;
                        };
        }
    }

    // Partially Adapted From aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/document/ItemUtils.java
    public static <T> T toSimpleValue(AttributeValue value)
    {
        if (value == null) {
            return null;
        }
        EnhancedAttributeValue enhancedAttributeValue = EnhancedAttributeValue.fromAttributeValue(value);
        T result = null;
        Object child = null;

        switch (enhancedAttributeValue.type()) {
            case NULL:
                break;
            case BOOL:
                result = (T) Boolean.valueOf(enhancedAttributeValue.asBoolean());
                break;
            case S:
                result = (T) StringAttributeConverter.create().transformTo(value);
                break;
            case N:
                result = (T) BigDecimalAttributeConverter.create().transformTo(value);
                break;
            case B:
                result = (T) enhancedAttributeValue.asBytes().asByteArray();
                break;
            case SS:
                result = (T) enhancedAttributeValue.asSetOfStrings();
                break;
            case NS:
                result = (T) value.ns().stream().map(BigDecimal::new).collect(Collectors.toList());
                break;
            case BS:
                result = (T) value.bs().stream().map(sdkBytes -> sdkBytes.asByteArray()).collect(Collectors.toList());
                break;
            case L:
                result = handleListAttribute(enhancedAttributeValue);
                break;
            case M:
                result = handleMapAttribute(enhancedAttributeValue);
                break;
        }
        return result;
    }

    private static <T> T handleMapAttribute(EnhancedAttributeValue enhancedAttributeValue)
    {
        Map<String, AttributeValue> valueMap = enhancedAttributeValue.asMap();
        if (valueMap.isEmpty()) {
            return (T) Collections.emptyMap();
        }
        Map<String, Object> result = new HashMap<>(valueMap.size());
        for (Map.Entry<String, AttributeValue> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            AttributeValue attributeValue = entry.getValue();
            result.put(key, toSimpleValue(attributeValue));
        }
        return (T) result;
    }

    private static <T> T handleListAttribute(EnhancedAttributeValue enhancedAttributeValue)
    {
        List<Object> result =
                enhancedAttributeValue.asListOfAttributeValues().stream().map(attributeValue -> toSimpleValue(attributeValue)).collect(Collectors.toList());
        return (T) result;
    }

    // Partially Adapted From aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/document/ItemUtils.java
    public static AttributeValue toAttributeValue(Object value)
    {
        if (value == null) {
            return AttributeValue.builder().nul(true).build();
        }
        else if (value instanceof String) {
            return StringAttributeConverter.create().transformFrom((String) value);
        }
        else if (value instanceof Boolean) {
            return BooleanAttributeConverter.create().transformFrom((Boolean) value);
        }
        else if (value instanceof BigDecimal) {
            return BigDecimalAttributeConverter.create().transformFrom((BigDecimal) value);
        }
        else if (value instanceof Number) {
            // For other numbers, we can convert them to BigDecimal first
            return BigDecimalAttributeConverter.create().transformFrom(BigDecimal.valueOf(((Number) value).doubleValue()));
        }
        else if (value instanceof byte[]) {
            return ByteArrayAttributeConverter.create().transformFrom((byte[]) value);
        }
        else if (value instanceof ByteBuffer) {
            SdkBytes b = SdkBytes.fromByteBuffer((ByteBuffer) value);
            return AttributeValue.builder().b(b).build();
        }
        else if (value instanceof Set<?>) {
            return handleSetType((Set<?>) value);
        }
        else if (value instanceof List<?>) {
            return handleListType((List<?>) value);
        }
        else if (value instanceof Map<?, ?>) {
            return handleMapType((Map<String, Object>) value);
        }
        else {
            throw new UnsupportedOperationException("Unsupported value type: " + value.getClass());
        }
    }

    public static String attributeToJson(AttributeValue attributeValue, String key)
    {
        EnhancedDocument enhancedDocument = EnhancedDocument.fromAttributeValueMap(ImmutableMap.of(key, attributeValue));
        return enhancedDocument.toJson();
    }

    public static AttributeValue jsonToAttributeValue(String jsonString, String key)
    {
        EnhancedDocument enhancedDocument = EnhancedDocument.fromJson(jsonString);
        if (!enhancedDocument.isPresent(key)) {
            throw new RuntimeException("Unknown attribute Key");
        }
        return enhancedDocument.toMap().get(key);
    }

    private static AttributeValue handleSetType(Set<?> value)
    {
        // Check the type of the first element as a sample
        Object firstElement = value.iterator().next();
        if (firstElement instanceof String) {
            // Handle String Set
            Set<String> stringSet = value.stream().map(e -> (String) e).collect(Collectors.toSet());
            return AttributeValue.builder().ss(stringSet).build();
        }
        else if (firstElement instanceof Number) {
            // Handle Number Set
            Set<String> numberSet = value.stream()
                    .map(e -> String.valueOf(((Number) e).doubleValue())) // Convert numbers to strings
                    .collect(Collectors.toSet());
            return AttributeValue.builder().ns(numberSet).build();
        } // Add other types if needed

        // Fallback for unsupported set types
        throw new UnsupportedOperationException("Unsupported Set element type: " + firstElement.getClass());
    }

    private static AttributeValue handleListType(List<?> value)
    {
        List<AttributeValue> attributeList = new ArrayList<>();
        for (Object element : value) {
            attributeList.add(toAttributeValue(element));
        }
        return AttributeValue.builder().l(attributeList).build();
    }

    private static AttributeValue handleMapType(Map<String, Object> value)
    {
        Map<String, AttributeValue> attributeMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            // Convert each value in the map to an AttributeValue
            attributeMap.put(entry.getKey(), toAttributeValue(entry.getValue()));
        }
        return AttributeValue.builder().m(attributeMap).build();
    }
}
