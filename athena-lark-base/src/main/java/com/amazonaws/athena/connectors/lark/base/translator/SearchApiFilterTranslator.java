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

import com.amazonaws.athena.connector.lambda.domain.predicate.AllOrNoneValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.lark.base.BaseConstants.RESERVED_SPLIT_KEY;

/**
 * Translates Athena constraints into Lark Bitable Search API JSON filter format.
 *
 * @see "https://open.larksuite.com/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-record/search"
 * @see "https://open.larksuite.com/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-record/record-filter-guide"
 */
public final class SearchApiFilterTranslator
{
    private static final Logger logger = LoggerFactory.getLogger(SearchApiFilterTranslator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SearchApiFilterTranslator()
    {
    }

    /**
     * Converts Athena constraints to Search API JSON filter format.
     *
     * @param constraints Map of field names to value sets from Athena query
     * @param fieldNameMappings Athena to Lark field mappings
     * @return JSON filter string, or empty string if no valid constraints
     */
    public static String toFilterJson(Map<String, ValueSet> constraints, List<AthenaFieldLarkBaseMapping> fieldNameMappings)
    {
        if (constraints == null || constraints.isEmpty()) {
            return "";
        }

        List<Map<String, Object>> allConditions = new ArrayList<>();

        for (Map.Entry<String, ValueSet> entry : constraints.entrySet()) {
            String lowercaseColumnName = entry.getKey();
            ValueSet valueSet = entry.getValue();

            AthenaFieldLarkBaseMapping mapping = findMappingForColumn(lowercaseColumnName, fieldNameMappings);
            if (mapping == null) {
                logger.warn("No mapping found for column: '{}'. Skipping.", lowercaseColumnName);
                continue;
            }

            String fieldName = mapping.larkBaseFieldName();
            UITypeEnum fieldUiType = mapping.nestedUIType().uiType();

            if (!isUiTypeAllowedForPushdown(fieldUiType)) {
                logger.info("Skipping pushdown for column '{}' - UI type {} not supported", fieldName, fieldUiType);
                continue;
            }

            List<Map<String, Object>> conditions = translateValueSetToConditions(fieldName, valueSet, fieldUiType);
            allConditions.addAll(conditions);
        }

        if (allConditions.isEmpty()) {
            return "";
        }

        // Build filter structure
        Map<String, Object> filter = new HashMap<>();
        filter.put("conjunction", "and");
        filter.put("conditions", allConditions);

        try {
            return OBJECT_MAPPER.writeValueAsString(filter);
        }
        catch (Exception e) {
            logger.error("Failed to serialize filter to JSON: {}", e.getMessage(), e);
            return "";
        }
    }

    /**
     * Converts Athena ORDER BY clause to Search API sort format.
     *
     * @param orderByFields List of ORDER BY fields from Athena query
     * @param fieldNameMappings Athena to Lark field mappings
     * @return JSON sort string, or empty string if no valid sort fields
     */
    public static String toSortJson(List<OrderByField> orderByFields, List<AthenaFieldLarkBaseMapping> fieldNameMappings)
    {
        if (orderByFields == null || orderByFields.isEmpty()) {
            return "";
        }

        List<Map<String, Object>> sortList = new ArrayList<>();

        for (OrderByField field : orderByFields) {
            String lowercaseColumnName = field.getColumnName();
            String originalColumnName = getOriginalColumnName(lowercaseColumnName, fieldNameMappings);

            if (originalColumnName == null || originalColumnName.isEmpty()) {
                logger.warn("Skipping ORDER BY for null/empty column name from: {}", lowercaseColumnName);
                continue;
            }

            Map<String, Object> sortItem = new HashMap<>();
            sortItem.put("field_name", originalColumnName);
            sortItem.put("desc", field.getDirection().name().contains("DESC"));

            sortList.add(sortItem);
        }

        if (sortList.isEmpty()) {
            return "";
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(sortList);
        }
        catch (Exception e) {
            logger.error("Failed to serialize sort to JSON: {}", e.getMessage(), e);
            return "";
        }
    }

    private static List<Map<String, Object>> translateValueSetToConditions(String fieldName, ValueSet valueSet, UITypeEnum fieldUiType)
    {
        List<Map<String, Object>> conditions = new ArrayList<>();

        if (valueSet instanceof SortedRangeSet srs) {
            conditions.addAll(translateRangeSet(fieldName, srs, fieldUiType));
        }
        else if (valueSet instanceof EquatableValueSet evs) {
            conditions.addAll(translateEquatableValueSet(fieldName, evs, fieldUiType));
        }
        else if (valueSet instanceof AllOrNoneValueSet aon) {
            if (!aon.isAll() && fieldUiType != UITypeEnum.CHECKBOX) {
                // IS NULL
                conditions.add(createCondition(fieldName, "isEmpty", null));
            }
        }

        return conditions;
    }

    private static List<Map<String, Object>> translateRangeSet(String fieldName, SortedRangeSet rangeSet, UITypeEnum fieldUiType)
    {
        List<Map<String, Object>> conditions = new ArrayList<>();

        // Handle single value (equality)
        if (rangeSet.isSingleValue()) {
            Object value = rangeSet.getSingleValue();

            // Special handling for checkbox NULL -> false
            if (value == null && fieldUiType == UITypeEnum.CHECKBOX) {
                conditions.add(createCondition(fieldName, "is", false));
                return conditions;
            }

            Object convertedValue = convertValueForSearchApi(value, fieldUiType);
            conditions.add(createCondition(fieldName, "is", convertedValue));
            return conditions;
        }

        // Handle IS NOT NULL
        if (!rangeSet.isNullAllowed()) {
            boolean isNotNull = isEffectivelyNotNull(rangeSet);
            if (isNotNull) {
                if (fieldUiType == UITypeEnum.CHECKBOX) {
                    conditions.add(createCondition(fieldName, "is", true));
                }
                else {
                    conditions.add(createCondition(fieldName, "isNotEmpty", null));
                }
                return conditions;
            }
        }

        // Handle ranges (>, <, >= , <= )
        try {
            List<Range> ranges = rangeSet.getRanges().getOrderedRanges();
            if (ranges != null) {
                for (Range range : ranges) {
                    Marker low = range.getLow();
                    Marker high = range.getHigh();

                    if (!low.isLowerUnbounded()) {
                        String operator = (low.getBound() == Marker.Bound.EXACTLY) ? "isGreaterEqual" : "isGreater";
                        Object value = convertValueForSearchApi(low.getValue(), fieldUiType);
                        conditions.add(createCondition(fieldName, operator, value));
                    }

                    if (!high.isUpperUnbounded()) {
                        String operator = (high.getBound() == Marker.Bound.EXACTLY) ? "isLessEqual" : "isLess";
                        Object value = convertValueForSearchApi(high.getValue(), fieldUiType);
                        conditions.add(createCondition(fieldName, operator, value));
                    }
                }
            }
        }
        catch (Exception e) {
            logger.warn("Error processing ranges for field '{}': {}", fieldName, e.getMessage());
        }

        return conditions;
    }

    private static List<Map<String, Object>> translateEquatableValueSet(String fieldName, EquatableValueSet valueSet, UITypeEnum fieldUiType)
    {
        List<Map<String, Object>> conditions = new ArrayList<>();
        boolean isWhiteList = valueSet.isWhiteList();
        String operator = isWhiteList ? "is" : "isNot";

        int valueCount = valueSet.getValueBlock().getRowCount();
        for (int i = 0; i < valueCount; i++) {
            Object value = valueSet.getValue(i);
            Object convertedValue = convertValueForSearchApi(value, fieldUiType);
            conditions.add(createCondition(fieldName, operator, convertedValue));
        }

        return conditions;
    }

    private static Map<String, Object> createCondition(String fieldName, String operator, Object value)
    {
        Map<String, Object> condition = new HashMap<>();
        condition.put("field_name", fieldName);
        condition.put("operator", operator);

        // Value must be an array for the search API
        if (value != null) {
            if (operator.equals("isEmpty") || operator.equals("isNotEmpty")) {
                // These operators don't need values
                condition.put("value", Collections.emptyList());
            }
            else {
                List<Object> valueArray = new ArrayList<>();
                valueArray.add(convertToString(value));
                condition.put("value", valueArray);
            }
        }
        else {
            condition.put("value", Collections.emptyList());
        }

        return condition;
    }

    private static Object convertValueForSearchApi(Object value, UITypeEnum fieldUiType)
    {
        if (value == null) {
            return "";
        }

        // Checkbox: convert boolean to boolean (not to 1/0)
        if (fieldUiType == UITypeEnum.CHECKBOX && value instanceof Boolean) {
            return value;
        }

        return value;
    }

    private static String convertToString(Object value)
    {
        if (value == null) {
            return "";
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        return String.valueOf(value);
    }

    private static boolean isEffectivelyNotNull(SortedRangeSet rangeSet)
    {
        try {
            List<Range> ranges = rangeSet.getRanges().getOrderedRanges();
            if (ranges != null && ranges.size() == 1) {
                Range onlyRange = ranges.get(0);
                if (onlyRange.getLow().isNullValue() && onlyRange.getLow().getBound() == Marker.Bound.ABOVE &&
                        onlyRange.getHigh().isNullValue() && onlyRange.getHigh().getBound() == Marker.Bound.BELOW) {
                    return true;
                }
            }

            Range span = rangeSet.getSpan();
            if (span != null && span.getLow().isLowerUnbounded() && span.getHigh().isUpperUnbounded()) {
                return true;
            }
        }
        catch (Exception e) {
            logger.debug("Error checking IS NOT NULL pattern: {}", e.getMessage());
        }
        return false;
    }

    private static boolean isUiTypeAllowedForPushdown(UITypeEnum uiType)
    {
        return switch (uiType) {
            case TEXT, BARCODE, SINGLE_SELECT, PHONE, NUMBER, PROGRESS, CURRENCY, RATING, CHECKBOX, EMAIL,
                 DATE_TIME, CREATED_TIME, MODIFIED_TIME -> true;
            default -> false;
        };
    }

    private static AthenaFieldLarkBaseMapping findMappingForColumn(String lowercaseAthenaName, List<AthenaFieldLarkBaseMapping> mappings)
    {
        if (mappings == null || lowercaseAthenaName == null) {
            return null;
        }
        return mappings.stream()
                .filter(m -> lowercaseAthenaName.equals(m.athenaName()))
                .findFirst()
                .orElse(null);
    }

    private static String getOriginalColumnName(String lowercaseColumnName, List<AthenaFieldLarkBaseMapping> fieldNameMappings)
    {
        if (lowercaseColumnName == null || lowercaseColumnName.isEmpty()) {
            return "";
        }
        if (fieldNameMappings == null) {
            return lowercaseColumnName;
        }
        return fieldNameMappings.stream()
                .filter(mapping -> lowercaseColumnName.equals(mapping.athenaName()))
                .map(AthenaFieldLarkBaseMapping::larkBaseFieldName)
                .findFirst()
                .orElse(lowercaseColumnName);
    }

    /**
     * Combines an existing filter with a split range filter for parallel processing.
     * Creates conditions for: splitKey >= startIndex AND splitKey <= endIndex
     */
    public static String toSplitFilterJson(String existingFilterJson, long startIndex, long endIndex)
    {
        if (startIndex <= 0 || endIndex <= 0) {
            return existingFilterJson;
        }

        try {
            List<Map<String, Object>> allConditions = new ArrayList<>();

            // Parse existing filter if present
            if (existingFilterJson != null && !existingFilterJson.isBlank()) {
                Map<String, Object> existingFilter = OBJECT_MAPPER.readValue(existingFilterJson, Map.class);
                List<Map<String, Object>> existingConditions = (List<Map<String, Object>>) existingFilter.get("conditions");
                if (existingConditions != null) {
                    allConditions.addAll(existingConditions);
                }
            }

            // Add split range conditions
            Map<String, Object> startCondition = new HashMap<>();
            startCondition.put("field_name", RESERVED_SPLIT_KEY);
            startCondition.put("operator", "isGreaterEqual");
            startCondition.put("value", List.of(String.valueOf(startIndex)));

            Map<String, Object> endCondition = new HashMap<>();
            endCondition.put("field_name", RESERVED_SPLIT_KEY);
            endCondition.put("operator", "isLessEqual");
            endCondition.put("value", List.of(String.valueOf(endIndex)));

            allConditions.add(startCondition);
            allConditions.add(endCondition);

            // Build combined filter
            Map<String, Object> filter = new HashMap<>();
            filter.put("conjunction", "and");
            filter.put("conditions", allConditions);

            return OBJECT_MAPPER.writeValueAsString(filter);
        }
        catch (Exception e) {
            logger.error("Failed to build split filter JSON", e);
            return existingFilterJson;
        }
    }
}
