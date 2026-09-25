/*-
 * #%L
 * athena-lark-base
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.translator;

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.amazonaws.athena.connectors.lark.base.resolver.LarkBaseFieldResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Utility class responsible for registering appropriate field extractors and writers
 * with a GeneratedRowWriter based on the Arrow schema definition for Lark Base data.
 */
public class RegistererExtractor
{
    private static final Logger logger = LoggerFactory.getLogger(RegistererExtractor.class);

    // Excel epoch constants
    private static final long EXCEL_EPOCH_DAY_OFFSET = 25569 - 2;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);

    // Timestamp detection thresholds
    private static final long TIMESTAMP_MILLIS_THRESHOLD = 10_000_000_000L; // ~March 1973
    private static final long TIMESTAMP_SECONDS_THRESHOLD = 100_000;
    private static final long SECONDS_TO_MILLIS = 1000L;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final Map<String, NestedUIType> larkFieldTypeMapping;

    public RegistererExtractor(Map<String, NestedUIType> larkFieldTypeMapping)
    {
        this.larkFieldTypeMapping = larkFieldTypeMapping != null ? larkFieldTypeMapping : Collections.emptyMap();
    }

    /**
     * Registers extractors and field writers for all fields in the provided schema.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param schema The Arrow schema defining the target structure.
     */
    public void registerExtractorsForSchema(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Schema schema)
    {
        for (Field field : schema.getFields()) {
            ArrowType arrowType = field.getType();

            // Check for Timestamp Arrow type (timestamptz from Glue)
            if (arrowType instanceof ArrowType.Timestamp) {
                registerTimestampMilliExtractor(rowWriterBuilder, field);
                continue;
            }

            Types.MinorType fieldType = Types.getMinorTypeForArrowType(arrowType);
            switch (fieldType) {
                case BIT:
                    // Checkbox
                    registerBitExtractor(rowWriterBuilder, field);
                    break;
                case TINYINT:
                    // Rating
                    registerTinyIntExtractor(rowWriterBuilder, field);
                    break;
                case VARCHAR:
                    // Text, Barcode, Single Select, Phone, Auto Number, Formula
                    registerVarCharExtractor(rowWriterBuilder, field);
                    break;
                case DECIMAL:
                    // Number, Progress, Currency
                    registerDecimalExtractor(rowWriterBuilder, field);
                    break;
                case DATEMILLI:
                    // Date Time, Created Time, Modified Time (from Glue timestamp type)
                    registerDateMilliExtractor(rowWriterBuilder, field);
                    break;
                case LIST:
                    registerListFieldWriterFactory(rowWriterBuilder, field);
                    break;
                case STRUCT:
                    registerStructFieldWriterFactory(rowWriterBuilder, field);
                    break;
                default:
                    logger.warn("No specific extractor or factory registered for field '{}' with Arrow type {}. Relying on default GeneratedRowWriter behavior or custom resolver if used by default.", field.getName(), fieldType);
                    break;
            }
        }
    }

    /**
     * Safely retrieves the context map from the raw context object provided to extractors.
     * Logs a warning and returns an empty map if the context is not of the expected type.
     *
     * @param context The raw context object.
     * @return A Map<String, Object> representing the row data, or an empty map if conversion fails.
     */
    private Map<String, Object> getContextMap(Object context)
    {
        if (!(context instanceof Map<?, ?> rawMap)) {
            return Collections.emptyMap();
        }

        Map<String, Object> item = new HashMap<>();
        for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
            if (entry.getKey() instanceof String) {
                item.put((String) entry.getKey(), entry.getValue());
            }
        }
        return item;
    }

    /**
     * Unwraps FORMULA field values from {type, value} wrapper structure.
     * FORMULA fields in Lark API return data wrapped in: {type: N, value: [...]}
     * This method extracts the actual value based on the childType.
     *
     * @param rawValue The raw value from API response
     * @param larkTypeInfo The field type information from schema
     * @return Unwrapped value, or original rawValue if not a FORMULA field
     */
    private Object unwrapFormula(Object rawValue, NestedUIType larkTypeInfo)
    {
        // Check if this is a FORMULA field
        if (larkTypeInfo != null && larkTypeInfo.uiType() == UITypeEnum.FORMULA) {
            // Check if has {type, value} structure
            if (rawValue instanceof Map<?, ?> map && map.containsKey("value")) {
                Object valueObj = map.get("value");
                if (valueObj instanceof List<?> valueList && !valueList.isEmpty()) {
                    UITypeEnum childType = larkTypeInfo.childType();

                    // Use LarkBaseTypeUtils to determine if childType is a LIST type
                    NestedUIType childTypeInfo = new NestedUIType(childType, UITypeEnum.UNKNOWN);
                    com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping tempMapping =
                            new com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping("temp", "temp", childTypeInfo);
                    Types.MinorType minorType = com.amazonaws.athena.connectors.lark.base.util.LarkBaseTypeUtils.larkFieldToArrowMinorType(tempMapping);

                    // If childType is LIST or TEXT, return whole array
                    if (minorType == Types.MinorType.LIST || childType == UITypeEnum.TEXT) {
                        return valueList;
                    }
                    // Otherwise return first element for scalar types
                    else {
                        return valueList.get(0);
                    }
                }
            }
        }
        return rawValue;
    }

    /**
     * Registers an extractor for Arrow TinyInt type.
     * Handles conversion from Boolean, Number, or String ("true"/"false"/numeric) to byte (0 or 1).
     * Leaves the value as SQL NULL (isSet=0) if input is null or conversion fails - RATING fields can be
     * genuinely unset in Lark, and defaulting that to 0 made it indistinguishable from an explicit zero
     * rating and made "IS NULL" pushdown/queries always return zero rows.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (TinyInt).
     */
    private void registerTinyIntExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) -> {
            dst.isSet = 0;
            String fieldName = field.getName();
            Map<String, Object> item = getContextMap(context);
            Object value = item.get(fieldName);

            if (value == null) {
                return;
            }

            try {
                if (value instanceof Boolean) {
                    dst.value = (byte) (((Boolean) value) ? 1 : 0);
                    dst.isSet = 1;
                }
                else if (value instanceof Number) {
                    dst.value = ((Number) value).byteValue();
                    dst.isSet = 1;
                }
                else if (value instanceof String strValue) {
                    if ("true".equalsIgnoreCase(strValue)) {
                        dst.value = (byte) 1;
                        dst.isSet = 1;
                    }
                    else if ("false".equalsIgnoreCase(strValue)) {
                        dst.value = (byte) 0;
                        dst.isSet = 1;
                    }
                    else if (!strValue.isEmpty()) {
                        dst.value = Byte.parseByte(strValue);
                        dst.isSet = 1;
                    }
                }
            }
            catch (Exception e) {
                dst.isSet = 0;
            }
        });
    }

    /**
     * Registers an extractor for Arrow Bit type (boolean).
     * Handles conversion from Boolean, Number (non-zero is true), or String ("true").
     * Leaves the value as SQL NULL (isSet=0) if input is null - CHECKBOX fields can be genuinely unset in
     * Lark, and defaulting that to false made it indistinguishable from an explicit unchecked box and made
     * "IS NULL" pushdown/queries always return zero rows.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (Bit).
     */
    private void registerBitExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (BitExtractor) (Object context, NullableBitHolder dst) -> {
            Map<String, Object> item = getContextMap(context);
            Object value = item.get(field.getName());

            if (value == null) {
                dst.isSet = 0;
                return;
            }

            dst.isSet = 1;
            dst.value = (value instanceof Boolean && ((Boolean) value)) ? 1 : 0;
        });
    }

    /**
     * Registers an extractor for Arrow VarChar type (String).
     * Handles conversion from various types (including complex ones like Map/List for _text fields) to String.
     * Sets value to null (isSet=0) if input is null. Uses String.valueOf() as a fallback.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (VarChar).
     */
    private void registerVarCharExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (VarCharExtractor) (Object context, NullableVarCharHolder dst) -> {
            dst.isSet = 0;
            String athenaFieldName = field.getName();
            Map<String, Object> recordMap = getContextMap(context);
            Object rawValue = recordMap.get(athenaFieldName);

            if (rawValue == null) {
                return;
            }

            String outputValue = null;
            NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(athenaFieldName);

            try {
                // Unwrap FORMULA fields first
                Object unwrappedValue = unwrapFormula(rawValue, larkTypeInfo);

                // unwrapFormula can itself return null: for a FORMULA field, Lark's {type, value: [...]}
                // wrapper's first element can genuinely be null (e.g. a formula like
                // IF(cond, linked_record, BLANK()) evaluating to blank for this row), and unwrapFormula
                // returns that element as-is with no null-check. Without this check, none of the
                // instanceof branches below match null, so outputValue falls through to the
                // String.valueOf(unwrappedValue) fallback - and String.valueOf(null) returns the literal
                // 4-character string "null", not a null reference, silently writing that text into the
                // column instead of leaving it as SQL NULL.
                if (unwrappedValue == null) {
                    return;
                }

                // BaseConstants.DOES_ACTIVATE_COMPLEX_TYPE_AS_JSON_STRING_ENV_VAR redirects what would
                // otherwise be a List/Struct-shaped column (MULTI_SELECT, USER, ATTACHMENT, URL, ...) to
                // VARCHAR at the schema level, so this extractor is now the one that runs for those raw
                // Map/List values too. Dispatch on the field's REAL declared UI type (not the value's
                // shape) before any of the TEXT-specific branches below - URL's own raw shape
                // ({"link":..., "text":..., "type":...}) would otherwise collide with the "TEXT field with
                // a single map" heuristic and silently discard everything but the "text" value.
                UITypeEnum effectiveUiType = larkTypeInfo != null && larkTypeInfo.uiType() == UITypeEnum.FORMULA
                        ? larkTypeInfo.childType() : (larkTypeInfo != null ? larkTypeInfo.uiType() : null);
                if (effectiveUiType != null && effectiveUiType.isComplexContainerType()) {
                    dst.value = JSON_MAPPER.writeValueAsString(unwrappedValue);
                    dst.isSet = 1;
                    return;
                }

                if (unwrappedValue instanceof String) {
                    outputValue = (String) unwrappedValue;
                }
                // Handle TEXT fields: {text: "...", type: "text"} (single map)
                else if (unwrappedValue instanceof Map<?, ?> mapElement && mapElement.containsKey("text")) {
                    Object textVal = mapElement.get("text");
                    outputValue = (textVal != null) ? String.valueOf(textVal) : null;
                }
                // Handle TEXT fields with multiple segments: [{text: "...", type: "text"}, {text: "...", type: "mention", ...}, ...]
                // Lark splits a cell into several segments when the text contains @mentions or links mixed with
                // plain text. Every segment (text or mention) carries a "text" key with its display value, so all
                // segments must be concatenated - otherwise everything after the first segment is silently dropped.
                else if (unwrappedValue instanceof List<?> listVal && !listVal.isEmpty()) {
                    StringBuilder segmentsBuilder = new StringBuilder();
                    for (Object element : listVal) {
                        if (element instanceof Map<?, ?> mapElement && mapElement.containsKey("text")) {
                            Object textVal = mapElement.get("text");
                            if (textVal != null) {
                                segmentsBuilder.append(textVal);
                            }
                        }
                        else {
                            segmentsBuilder.append(String.valueOf(element));
                        }
                    }
                    // Fall back to the raw list representation only if every segment yielded nothing
                    // (e.g. a single segment with a null "text" value), matching the single-map fallback below.
                    outputValue = (segmentsBuilder.length() > 0) ? segmentsBuilder.toString() : null;
                }

                if (outputValue == null) {
                    outputValue = String.valueOf(unwrappedValue);
                }

                if (outputValue != null) {
                    dst.value = outputValue;
                    dst.isSet = 1;
                }
            }
            catch (Exception e) {
                logger.error("VarCharExtractor: Error for field '{}', raw value type {}: {}. Value: {}",
                        athenaFieldName, rawValue.getClass().getName(), e.getMessage(), rawValue, e);
                dst.isSet = 0;
            }
        });
    }

    /**
     * Registers an extractor for Arrow Decimal type.
     * Handles conversion from BigDecimal, Number, or String.
     * Leaves the value as SQL NULL (isSet=0) if input is null or conversion fails - NUMBER/CURRENCY/PROGRESS
     * fields can be genuinely unset in Lark, and defaulting that to 0 made it indistinguishable from an
     * explicit zero and made "IS NULL" pushdown/queries always return zero rows even when Lark's own data
     * has the field set to null (confirmed via a direct Bitable API read of a Lark Base test record).
     * Note: Precision/scale from the Field definition are used by the writer, not explicitly checked here.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (Decimal).
     */
    private void registerDecimalExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (DecimalExtractor) (Object context, NullableDecimalHolder dst) -> {
            dst.isSet = 0;
            String fieldName = field.getName();
            Map<String, Object> item = getContextMap(context);
            Object rawValue = item.get(fieldName);

            if (rawValue == null) {
                return;
            }

            try {
                // Unwrap FORMULA fields first
                NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(fieldName);
                Object value = unwrapFormula(rawValue, larkTypeInfo);

                if (value instanceof BigDecimal) {
                    dst.value = (BigDecimal) value;
                    dst.isSet = 1;
                }
                else if (value instanceof Number) {
                    dst.value = new BigDecimal(value.toString());
                    dst.isSet = 1;
                }
                else if (value instanceof String strValue) {
                    if (!strValue.isEmpty()) {
                        dst.value = new BigDecimal(strValue);
                        dst.isSet = 1;
                    }
                }
            }
            catch (Exception e) {
                dst.isSet = 0;
            }
        });
    }

    /**
     * Converts a Lark numeric value to Unix milliseconds.
     * Handles three formats:
     * 1. Unix Timestamps in milliseconds (numbers > 10^10)
     * 2. Unix Timestamps in seconds (numbers > 100,000, converted to milliseconds)
     * 3. Excel-like serial date numbers (days since 1900-01-01, converted to Unix milliseconds)
     *
     * @param numValue The numeric value to convert
     * @param fieldName Field name for logging
     * @param extractorType Type of extractor calling this method (for logging)
     * @return Converted timestamp in milliseconds, or null if value is zero
     */
    private Long convertToTimestampMillis(Number numValue, String fieldName, String extractorType)
    {
        long longValue = numValue.longValue();
        double doubleValue = numValue.doubleValue();

        if (longValue == 0L && doubleValue == 0.0) {
            return null;
        }

        if (longValue > TIMESTAMP_MILLIS_THRESHOLD) {
            logger.info("{}: Field={}, Value={}, Writing as milliseconds", extractorType, fieldName, longValue);
            return longValue;
        }
        else if (longValue > TIMESTAMP_SECONDS_THRESHOLD) {
            logger.info("{}: Field={}, Value={}, Converting from seconds to milliseconds", extractorType, fieldName, longValue);
            return longValue * SECONDS_TO_MILLIS;
        }
        else {
            // Assume Excel-like serial date number
            long days = (long) doubleValue;
            double fractionalDay = doubleValue - days;
            long dateMillis = (days - EXCEL_EPOCH_DAY_OFFSET) * MILLIS_PER_DAY;
            long timeMillis = (long) (fractionalDay * MILLIS_PER_DAY);

            // Adjust for potential rounding errors near midnight
            timeMillis = Math.max(0, Math.min(timeMillis, MILLIS_PER_DAY - 1));

            logger.info("{}: Field={}, Value={}, Converting from Excel date", extractorType, fieldName, doubleValue);
            return dateMillis + timeMillis;
        }
    }

    /**
     * Registers an extractor for Arrow DateMilli type (used for timestamp fields).
     * Handles conversion from Lark's numeric date/timestamp formats to Unix milliseconds.
     * Sets value to null (isSet=0) if input is null, zero, or conversion fails.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (DateMilli).
     */
    private void registerDateMilliExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) -> {
            dst.isSet = 0;
            String fieldName = field.getName();
            Map<String, Object> item = getContextMap(context);
            Object rawValue = item.get(fieldName);

            if (rawValue == null) {
                return;
            }
            try {
                // Unwrap FORMULA fields first
                NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(fieldName);
                Object value = unwrapFormula(rawValue, larkTypeInfo);

                if (value instanceof Number numValue) {
                    Long timestamp = convertToTimestampMillis(numValue, fieldName, "DateMilliExtractor");
                    if (timestamp != null) {
                        dst.value = timestamp;
                        dst.isSet = 1;
                    }
                }
            }
            catch (Exception e) {
                logger.error("DateMilliExtractor: Error extracting date for field '{}': {}", fieldName, e.getMessage(), e);
                dst.isSet = 0;
            }
        });
    }

    /**
     * Registers an extractor for Arrow Timestamp type.
     * Handles conversion from Lark's numeric date/timestamp formats to Unix milliseconds.
     * Sets value to null (isSet=0) if input is null, zero, or conversion fails.
     *
     * @param rowWriterBuilder The builder for the GeneratedRowWriter.
     * @param field The Arrow field definition (Timestamp).
     */
    private void registerTimestampMilliExtractor(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        rowWriterBuilder.withExtractor(field.getName(), (BigIntExtractor) (Object context, NullableBigIntHolder dst) -> {
            dst.isSet = 0;
            String fieldName = field.getName();
            Map<String, Object> item = getContextMap(context);
            Object rawValue = item.get(fieldName);

            if (rawValue == null) {
                return;
            }
            try {
                // Unwrap FORMULA fields first
                NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(fieldName);
                Object value = unwrapFormula(rawValue, larkTypeInfo);

                if (value instanceof Number numValue) {
                    Long timestamp = convertToTimestampMillis(numValue, fieldName, "TimestampMilliExtractor");
                    if (timestamp != null) {
                        dst.value = timestamp;
                        dst.isSet = 1;
                    }
                }
            }
            catch (Exception e) {
                logger.error("TimestampMilliExtractor: Error extracting timestamp for field '{}': {}", fieldName, e.getMessage(), e);
                dst.isSet = 0;
            }
        });
    }

    /**
     * A chained LOOKUP (e.g. Lookup&lt;Lookup&lt;MultiSelect&gt;&gt;) produces an Arrow schema nested one
     * List level deeper per chain hop (List&lt;List&lt;Utf8&gt;&gt;, List&lt;List&lt;Struct&gt;&gt;, ...), but
     * Lark's API collapses a single-linked-record LOOKUP down to a bare scalar (String/Map) instead of
     * wrapping it in the full chain of Lists. Wrapping such a scalar exactly once (as this method's callers
     * do for the outermost level) leaves any inner List levels holding a raw scalar where a List was
     * expected, which is what threw the "String/LinkedHashMap cannot be cast to List" writer exceptions.
     * This walks the field's child-List chain below the outermost level and wraps once per level found,
     * so the final single-element structure matches the schema's actual nesting depth.
     */
    private static Object wrapScalarToMatchNestedListDepth(Object scalarValue, Field listField)
    {
        Object wrapped = scalarValue;
        Field current = listField;
        while (!current.getChildren().isEmpty() && current.getChildren().get(0).getType() instanceof ArrowType.List) {
            wrapped = List.of(wrapped);
            current = current.getChildren().get(0);
        }
        return wrapped;
    }

    /**
     * Extracts the display text from one linked record's raw per-element value for a LOOKUP&lt;Text&gt;
     * field. Lark represents a Text cell as either a single segment {@code Map} ({@code {"text": "...",
     * "type": "text"}}) or, when the cell mixes plain text with @mentions/links, a {@code List} of
     * several segment Maps that must be concatenated - the identical shape and reasoning already
     * handled for a direct TEXT field in the VarChar extractor above. Handling only the single-Map shape
     * here previously meant a linked record's text was silently dropped from the resulting array
     * whenever its target field happened to contain mentions/rich content, with no error or log line.
     *
     * @return The segment's display text, or null if the element doesn't match either known shape (or
     * yields no text), so the caller's {@code filter(Objects::nonNull)} drops it - preserving the prior
     * behavior for anything genuinely unrecognized.
     */
    private static String extractLookupTextSegment(Object element)
    {
        if (element instanceof Map<?, ?> mapElement) {
            return mapElement.containsKey("text") ? String.valueOf(mapElement.get("text")) : null;
        }
        if (element instanceof List<?> segments) {
            StringBuilder builder = new StringBuilder();
            for (Object segment : segments) {
                if (segment instanceof Map<?, ?> segmentMap && segmentMap.containsKey("text")) {
                    Object textVal = segmentMap.get("text");
                    if (textVal != null) {
                        builder.append(textVal);
                    }
                }
            }
            return !builder.isEmpty() ? builder.toString() : null;
        }
        return null;
    }

    private void registerListFieldWriterFactory(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        LarkBaseFieldResolver resolver = new LarkBaseFieldResolver();
        String fieldName = field.getName();
        NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(fieldName);

        rowWriterBuilder.withFieldWriterFactory(fieldName, (vector, extractor, constraint) ->
                (Object context, int rowNum) -> {
                    Map<String, Object> recordMap = getContextMap(context);
                    Object rawListValue = recordMap.get(fieldName);

                    if (rawListValue == null) {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return true;
                    }

                    // Unwrap FORMULA fields first
                    Object unwrappedValue = unwrapFormula(rawListValue, larkTypeInfo);

                    // unwrapFormula can itself return null: for a FORMULA field, Lark's {type, value: [...]}
                    // wrapper's first element can genuinely be null (e.g. a formula like
                    // IF(cond, linked_record, BLANK()) evaluating to blank for this row), and unwrapFormula
                    // returns that element as-is with no null-check. Without this check, the `else` branch
                    // below (the catch-all for "not List, not Map, not String") would call
                    // unwrappedValue.getClass() on a null reference and throw a NullPointerException,
                    // which propagates out of this row-writer lambda and silently drops the entire row
                    // (caught generically by BaseRecordHandler's per-row try/catch), not just this column.
                    if (unwrappedValue == null) {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return true;
                    }

                    // Handle case where Lark API returns Map or String instead of List for LINK/LOOKUP fields
                    List<?> listValue;
                    if (unwrappedValue instanceof List<?>) {
                        listValue = (List<?>) unwrappedValue;
                    }
                    else if (unwrappedValue instanceof Map) {
                        // LINK fields (SINGLE_LINK, DUPLEX_LINK) may return as Map instead of List
                        // Wrap it in a List to match the expected array schema
                        logger.debug("FieldWriterFactory for List field '{}': Got Map instead of List (likely LINK field). Wrapping in List.",
                                fieldName);
                        listValue = List.of(wrapScalarToMatchNestedListDepth(unwrappedValue, field));
                    }
                    else if (unwrappedValue instanceof String) {
                        // LOOKUP fields may return as String instead of List
                        // Wrap it in a List to match the expected array schema
                        logger.debug("FieldWriterFactory for List field '{}': Got String instead of List (likely LOOKUP field). Wrapping in List.",
                                fieldName);
                        listValue = List.of(wrapScalarToMatchNestedListDepth(unwrappedValue, field));
                    }
                    else {
                        logger.error("FieldWriterFactory for List field '{}': Expected List, Map, or String, got {}. Writing null.",
                                fieldName, unwrappedValue.getClass().getName());
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return true; // Changed from false to true to not skip the entire row
                    }

                    Object processedList = listValue;

                    if (larkTypeInfo != null && larkTypeInfo.uiType() == UITypeEnum.LOOKUP &&
                            larkTypeInfo.childType() == UITypeEnum.TEXT &&
                            field.getChildren().get(0).getType() instanceof ArrowType.Utf8) {
                        processedList = listValue.stream()
                                .map(RegistererExtractor::extractLookupTextSegment)
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                        logger.trace("FieldWriterFactory for Lookup<Text> field '{}': Transformed List<Map> to List<String>: {}", fieldName, processedList);
                    }

                    try {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, processedList);
                        return true;
                    }
                    catch (Exception e) {
                        logger.error("FieldWriterFactory for List field '{}': Error writing list. ProcessedList type: {}. Exception: {}",
                                fieldName, processedList.getClass().getName(), e.getMessage(), e);
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return false;
                    }
                });
    }

    private void registerStructFieldWriterFactory(GeneratedRowWriter.RowWriterBuilder rowWriterBuilder, Field field)
    {
        LarkBaseFieldResolver resolver = new LarkBaseFieldResolver();
        String fieldName = field.getName();
        NestedUIType larkTypeInfo = this.larkFieldTypeMapping.get(fieldName);

        rowWriterBuilder.withFieldWriterFactory(fieldName, (vector, extractor, constraint) ->
                (Object context, int rowNum) -> {
                    Map<String, Object> recordMap = getContextMap(context);
                    Object rawStructValue = recordMap.get(fieldName);

                    if (rawStructValue == null) {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return true;
                    }

                    // Unwrap FORMULA fields first
                    Object unwrappedValue = unwrapFormula(rawStructValue, larkTypeInfo);

                    // unwrapFormula can itself return null: for a FORMULA field, Lark's {type, value: [...]}
                    // wrapper's first element can genuinely be null (e.g. a formula like
                    // IF(cond, linked_record, BLANK()) evaluating to blank for this row), and unwrapFormula
                    // returns that element as-is with no null-check. Without this check, the
                    // "!(unwrappedValue instanceof Map)" branch below would call unwrappedValue.getClass()
                    // on a null reference and throw a NullPointerException, which propagates out of this
                    // row-writer lambda and silently drops the entire row (caught generically by
                    // BaseRecordHandler's per-row try/catch), not just this column.
                    if (unwrappedValue == null) {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return true;
                    }

                    if (!(unwrappedValue instanceof Map)) {
                        logger.error("FieldWriterFactory for Struct field '{}': Expected Map, got {}. Writing null.",
                                fieldName, unwrappedValue.getClass().getName());
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return false;
                    }

                    try {
                        BlockUtils.setComplexValue(vector, rowNum, resolver, unwrappedValue);
                        return true;
                    }
                    catch (Exception e) {
                        logger.error("FieldWriterFactory for Struct field '{}': Error writing struct. Value type: {}. Exception: {}",
                                fieldName, unwrappedValue.getClass().getName(), e.getMessage(), e);
                        BlockUtils.setComplexValue(vector, rowNum, resolver, null);
                        return false;
                    }
                });
    }
}
