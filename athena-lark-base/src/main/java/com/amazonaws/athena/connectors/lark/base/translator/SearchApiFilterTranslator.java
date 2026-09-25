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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        List<Map<String, Object>> orGroups = new ArrayList<>();

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

            // Lark's Search API restricts the "is"/"isNot" operators to a single value each (there is no native
            // "in" operator - it is documented as not yet supported), so an IN-clause with more than one value
            // cannot be expressed as multiple "is" conditions ANDed together - that would require the field to
            // equal every value simultaneously and would always match zero rows. Instead, express it as an
            // OR-group of single-value "is" conditions nested under the top-level AND via "children", which is
            // the pattern Lark's filter guide recommends. A NOT IN-clause (blacklist) is unaffected: multiple
            // "isNot" conditions ANDed together already correctly means "not equal to any of these values".
            if (valueSet instanceof EquatableValueSet equatableValueSet
                    && equatableValueSet.isWhiteList() && equatableValueSet.getValueBlock().getRowCount() > 1) {
                List<Map<String, Object>> orConditions = translateEquatableValueSet(fieldName, equatableValueSet, fieldUiType);
                Map<String, Object> orGroup = new HashMap<>();
                orGroup.put("conjunction", "or");
                orGroup.put("conditions", orConditions);
                orGroups.add(orGroup);
            }
            // A SortedRangeSet with more than one Range represents a UNION of ranges for this single column
            // (per the SDK's own definition: "col between 10 and 30, or col between 40 and 60, ..."), e.g.
            // "x < 5 OR x > 100", or "x != 5" (modeled as two disjoint ranges excluding the single point).
            // translateRangeSet's per-range loop would otherwise flatten every range's conditions into the
            // same top-level AND list - the exact same class of bug as the IN-clause case above, just for
            // ranges instead of discrete values. Route it into an OR-group instead when it's safely
            // expressible that way (see buildRangeUnionOrGroup for when it isn't).
            else if (valueSet instanceof SortedRangeSet rangeSet && !isSingleValueSafe(rangeSet)
                    && getOrderedRangesSafe(rangeSet, fieldName).size() > 1) {
                List<Range> ranges = getOrderedRangesSafe(rangeSet, fieldName);

                // A column with a natural ordering (e.g. VARCHAR) represents "col != X" / "col NOT IN
                // (x1, x2, ...)" as N+1 disjoint ranges excluding N points: (-inf, x1) union (x1, x2) union
                // ... union (xN, +inf). Routing that through the generic range-union path below would emit
                // isLess/isGreater (which Lark doesn't support for a categorical type like SINGLE_SELECT -
                // silently zero rows) and would fail buildRangeUnionOrGroup's single-bound check entirely
                // for N>1 (every interior range needs both bounds), skipping pushdown altogether. Recognize
                // this shape and emit one "isNot" per excluded point ANDed together instead, which needs no
                // ordering support and works for every equality-capable type.
                List<Object> excludedValues = tryGetExcludedValues(ranges);
                if (excludedValues != null) {
                    // CHECKBOX (Boolean has an ordering - false < true - so "!=" reaches this SortedRangeSet
                    // path too, not just EquatableValueSet) supports only "is", not "isNot" at all (confirmed
                    // live: `field_checkbox != true` returned zero rows instead of the real 280 false rows -
                    // see translateEquatableValueSet's blacklist case for the same fix on that code path).
                    // Negate the excluded boolean and push "is" with the opposite instead.
                    if (fieldUiType == UITypeEnum.CHECKBOX) {
                        for (Object excludedValue : excludedValues) {
                            if (excludedValue instanceof Boolean booleanValue) {
                                allConditions.add(createCondition(fieldName, "is", !booleanValue));
                            }
                        }
                        continue;
                    }

                    for (Object excludedValue : excludedValues) {
                        Object convertedValue = convertValueForSearchApi(excludedValue, fieldUiType);
                        allConditions.add(createCondition(fieldName, "isNot", convertedValue));
                    }
                    // Same 3-valued-logic gap as the NOT IN blacklist case (see translateEquatableValueSet):
                    // "col != X" never matches NULL in SQL, but Lark's "isNot" treats an empty field as
                    // trivially not-equal-to-X, so it leaks in unless explicitly excluded.
                    if (!rangeSet.isNullAllowed()) {
                        allConditions.add(createCondition(fieldName, "isNotEmpty", null));
                    }
                    continue;
                }

                // A genuine range union (e.g. "x < 5 OR x > 100", not a "!=" exclusion) needs isGreater/isLess,
                // which - same as the single-range case in translateRangeSet - only NUMBER/CURRENCY/PROGRESS/
                // RATING/DATE_TIME-family fields support in Lark's Search API.
                if (!isOrderableUiType(fieldUiType)) {
                    logger.info("Skipping pushdown for column '{}': UI type {} has no ordering operators in "
                            + "Lark's Search API. Falling back to client-side filtering.", fieldName, fieldUiType);
                    continue;
                }

                List<Map<String, Object>> orConditions = buildRangeUnionOrGroup(fieldName, ranges, fieldUiType);
                if (orConditions != null) {
                    Map<String, Object> orGroup = new HashMap<>();
                    orGroup.put("conjunction", "or");
                    orGroup.put("conditions", orConditions);
                    orGroups.add(orGroup);
                }
                else {
                    logger.info("Skipping pushdown for column '{}': multi-range union includes a range needing "
                            + "both bounds (e.g. BETWEEN), which Lark's filter API can't express as an OR "
                            + "without unsupported nested grouping. Falling back to client-side filtering.", fieldName);
                }
            }
            else {
                List<Map<String, Object>> conditions = translateValueSetToConditions(fieldName, valueSet, fieldUiType);
                allConditions.addAll(conditions);
            }
        }

        if (allConditions.isEmpty() && orGroups.isEmpty()) {
            return "";
        }

        // Build filter structure
        Map<String, Object> filter = new HashMap<>();
        filter.put("conjunction", "and");
        filter.put("conditions", allConditions);
        if (!orGroups.isEmpty()) {
            filter.put("children", orGroups);
        }

        try {
            return OBJECT_MAPPER.writeValueAsString(filter);
        }
        catch (Exception e) {
            logger.error("Failed to serialize filter to JSON: {}", e.getMessage(), e);
            return "";
        }
    }

    /**
     * Safely checks {@link SortedRangeSet#isSingleValue()}, treating any exception (e.g. from a malformed or
     * mocked ValueSet) as "not a single value" so the caller falls through to the general per-field error
     * handling instead of letting the exception escape {@link #toFilterJson}.
     */
    private static boolean isSingleValueSafe(SortedRangeSet rangeSet)
    {
        try {
            return rangeSet.isSingleValue();
        }
        catch (Exception e) {
            return false;
        }
    }

    /**
     * Safely retrieves the ordered ranges from a SortedRangeSet, returning an empty list (instead of letting
     * the exception escape {@link #toFilterJson}) if the underlying ValueSet throws.
     */
    private static List<Range> getOrderedRangesSafe(SortedRangeSet rangeSet, String fieldName)
    {
        try {
            List<Range> ranges = rangeSet.getRanges().getOrderedRanges();
            return ranges != null ? ranges : Collections.emptyList();
        }
        catch (Exception e) {
            logger.warn("Error retrieving ranges for field '{}': {}", fieldName, e.getMessage());
            return Collections.emptyList();
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

    /**
     * Adds an "isEmpty"/"isNotEmpty" condition on {@code larkFieldName} to an existing (possibly blank)
     * top-level AND filter, ANDing it with whatever conditions/OR-groups are already there. Used to split
     * a single ORDER BY ... NULLS FIRST fetch into two Lark requests - one for the null rows, one for the
     * Lark-sorted non-null rows - since Lark's own sort has no null-positioning control (see
     * {@code BaseMetadataHandler#findNullsFirstOriginalFieldName} and its use in {@code doGetSplits}).
     */
    @SuppressWarnings("unchecked")
    public static String addEmptinessCondition(String filterJson, String larkFieldName, boolean wantEmpty)
    {
        Map<String, Object> filter;
        if (filterJson == null || filterJson.isEmpty()) {
            filter = new HashMap<>();
            filter.put("conjunction", "and");
            filter.put("conditions", new ArrayList<Map<String, Object>>());
        }
        else {
            try {
                filter = OBJECT_MAPPER.readValue(filterJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() { });
                filter.putIfAbsent("conjunction", "and");
                filter.putIfAbsent("conditions", new ArrayList<Map<String, Object>>());
            }
            catch (Exception e) {
                logger.warn("Failed to parse existing filter JSON while adding {} condition for '{}': {}. "
                        + "Discarding the existing filter.", wantEmpty ? "isEmpty" : "isNotEmpty", larkFieldName, e.getMessage(), e);
                filter = new HashMap<>();
                filter.put("conjunction", "and");
                filter.put("conditions", new ArrayList<Map<String, Object>>());
            }
        }

        List<Map<String, Object>> conditions = (List<Map<String, Object>>) filter.get("conditions");
        conditions.add(createCondition(larkFieldName, wantEmpty ? "isEmpty" : "isNotEmpty", null));

        try {
            return OBJECT_MAPPER.writeValueAsString(filter);
        }
        catch (Exception e) {
            logger.error("Failed to serialize filter with {} condition to JSON: {}",
                    wantEmpty ? "isEmpty" : "isNotEmpty", e.getMessage(), e);
            return filterJson != null ? filterJson : "";
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

        // Handle single value (equality). A SortedRangeSet with zero ranges and nullAllowed=true is a pure
        // "IS NULL" constraint - the domain's only satisfying value is null, so isSingleValue() is true with
        // getSingleValue() == null. Checkbox has no separate empty state so NULL maps to "is false"; every
        // other type must use "isEmpty" - falling through to convertValueForSearchApi/"is" would otherwise
        // turn null into the literal empty string "" (convertValueForSearchApi's null branch), which Lark's
        // Search API treats as "equals empty string" and matches zero rows instead of the actual NULL rows.
        if (rangeSet.isSingleValue()) {
            Object value = rangeSet.getSingleValue();

            if (value == null) {
                if (fieldUiType == UITypeEnum.CHECKBOX) {
                    conditions.add(createCondition(fieldName, "is", false));
                }
                else {
                    conditions.add(createCondition(fieldName, "isEmpty", null));
                }
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
                    // CHECKBOX has no separate empty state in Lark - every row is genuinely true or
                    // false - so "IS NOT NULL" is not "equals true", it's a tautology that matches every
                    // row. Pushing "is true" here (as this used to) silently excluded every `false` row
                    // from the result: a plain `WHERE checkbox_col IS NOT NULL` would return only the
                    // `true` rows instead of all of them, with no error. Push no condition at all and let
                    // Athena's own engine apply the (always-true) check against the real materialized
                    // value - which RegistererExtractor's BitExtractor does correctly leave as SQL NULL
                    // only when the field is genuinely absent, never for an explicit `false`.
                    return conditions;
                }
                conditions.add(createCondition(fieldName, "isNotEmpty", null));
                return conditions;
            }
        }

        // Handle a single range (>, <, >=, <=, or BETWEEN via both bounds set). Callers (toFilterJson) route
        // SortedRangeSets with more than one Range to buildRangeUnionOrGroup instead, since multiple ranges
        // are a union (OR) that a flat AND list here would translate incorrectly - see toFilterJson.
        //
        // Per Lark's record-filter-guide, isGreater/isGreaterEqual/isLess/isLessEqual are only supported for
        // NUMBER/CURRENCY/PROGRESS/RATING and the DATE_TIME family - TEXT/BARCODE/PHONE/EMAIL/SINGLE_SELECT
        // have no ordering operators at all (confirmed live: `field_text > 'M'` and
        // `field_single_select > 'Option A'` both returned zero rows instead of the real match count).
        // A genuine range constraint on one of those types can't be pushed down at all; skip it and let
        // Athena's engine filter client-side instead of sending an operator Lark rejects.
        if (!isOrderableUiType(fieldUiType)) {
            logger.info("Skipping pushdown for column '{}': UI type {} has no ordering operators in Lark's "
                    + "Search API. Falling back to client-side filtering.", fieldName, fieldUiType);
            return conditions;
        }

        try {
            List<Range> ranges = rangeSet.getRanges().getOrderedRanges();
            if (ranges != null && ranges.size() == 1) {
                addRangeBoundConditions(conditions, fieldName, ranges.get(0), fieldUiType);
            }
        }
        catch (Exception e) {
            logger.warn("Error processing ranges for field '{}': {}", fieldName, e.getMessage());
        }

        return conditions;
    }

    /**
     * Per Lark's record-filter-guide, only these UI types support isGreater/isGreaterEqual/isLess/isLessEqual
     * (the DATE_TIME family further restricts this to isGreater/isLess only - see addRangeBoundConditions).
     * Every other pushdown-eligible type (TEXT, BARCODE, PHONE, EMAIL, SINGLE_SELECT) supports only equality,
     * "contains", and empty-checks - no ordering at all.
     */
    private static boolean isOrderableUiType(UITypeEnum uiType)
    {
        return uiType == UITypeEnum.NUMBER || uiType == UITypeEnum.CURRENCY || uiType == UITypeEnum.PROGRESS
                || uiType == UITypeEnum.RATING || isDateTimeUiType(uiType);
    }

    /**
     * Appends the low/high bound conditions for a single Range (e.g. {@code isGreater}/{@code isLessEqual}) to
     * the given conditions list. A range with both bounds set (e.g. BETWEEN) appends both conditions, which the
     * caller must AND together for correctness.
     */
    private static void addRangeBoundConditions(List<Map<String, Object>> conditions, String fieldName, Range range, UITypeEnum fieldUiType)
    {
        Marker low = range.getLow();
        Marker high = range.getHigh();
        // Confirmed against Lark's Search Records API directly: a DATE_TIME-family field rejects
        // isGreaterEqual/isLessEqual outright ("fieldType '5' not support isGreaterEqual"), so a BETWEEN's
        // normally-inclusive bounds must fall back to the strict isGreater/isLess instead - the boundary
        // instant itself won't match, which is an accepted platform limitation (Lark's own filter guide
        // notes date comparisons are truncated to day granularity anyway).
        boolean isDateTime = isDateTimeUiType(fieldUiType);

        if (!low.isLowerUnbounded()) {
            String operator = (!isDateTime && low.getBound() == Marker.Bound.EXACTLY) ? "isGreaterEqual" : "isGreater";
            Object value = convertValueForSearchApi(low.getValue(), fieldUiType);
            conditions.add(createCondition(fieldName, operator, value));
        }

        if (!high.isUpperUnbounded()) {
            String operator = (!isDateTime && high.getBound() == Marker.Bound.EXACTLY) ? "isLessEqual" : "isLess";
            Object value = convertValueForSearchApi(high.getValue(), fieldUiType);
            conditions.add(createCondition(fieldName, operator, value));
        }
    }

    /**
     * Detects the N+1-range shape a column with a natural ordering uses to represent {@code col != X} /
     * {@code col NOT IN (x1, ..., xN)}: {@code (-inf, x1) union (x1, x2) union ... union (xN, +inf)} - every
     * range single-bounded except the interior ones, which share their excluded value with both neighbors.
     *
     * @return The excluded values in order, or {@code null} if {@code ranges} isn't this shape.
     */
    private static List<Object> tryGetExcludedValues(List<Range> ranges)
    {
        if (!ranges.get(0).getLow().isLowerUnbounded()
                || !ranges.get(ranges.size() - 1).getHigh().isUpperUnbounded()) {
            return null;
        }

        List<Object> excludedValues = new ArrayList<>();
        for (int i = 0; i < ranges.size() - 1; i++) {
            Marker high = ranges.get(i).getHigh();
            Marker low = ranges.get(i + 1).getLow();

            boolean adjoins = !high.isUpperUnbounded() && high.getBound() == Marker.Bound.BELOW
                    && !low.isLowerUnbounded() && low.getBound() == Marker.Bound.ABOVE
                    && Objects.equals(high.getValue(), low.getValue());
            if (!adjoins) {
                return null;
            }
            excludedValues.add(high.getValue());
        }

        return excludedValues;
    }

    /**
     * Builds the conditions for an OR-group representing a union of multiple ranges on one column (e.g.
     * {@code x < 5 OR x > 100}, or {@code x != 5} modeled as two disjoint ranges excluding a single point).
     * Lark's filter API supports only one level of "children" nesting with a single conjunction per group, so
     * this can only be expressed correctly when every range needs just one condition (single-bounded). A range
     * needing both bounds ANDed together (e.g. BETWEEN) mixed into a union would require "OR of ANDs", which
     * needs two levels of nesting - not supported.
     *
     * @return The OR-group's conditions (one per range), or null if the union isn't safely expressible this way.
     */
    private static List<Map<String, Object>> buildRangeUnionOrGroup(String fieldName, List<Range> ranges, UITypeEnum fieldUiType)
    {
        boolean allSingleBounded = ranges.stream()
                .allMatch(range -> range.getLow().isLowerUnbounded() != range.getHigh().isUpperUnbounded());

        if (!allSingleBounded) {
            return null;
        }

        List<Map<String, Object>> conditions = new ArrayList<>();
        try {
            for (Range range : ranges) {
                addRangeBoundConditions(conditions, fieldName, range, fieldUiType);
            }
        }
        catch (Exception e) {
            logger.warn("Error processing range union for field '{}': {}", fieldName, e.getMessage());
            return null;
        }

        return conditions;
    }

    private static List<Map<String, Object>> translateEquatableValueSet(String fieldName, EquatableValueSet valueSet, UITypeEnum fieldUiType)
    {
        List<Map<String, Object>> conditions = new ArrayList<>();
        boolean isWhiteList = valueSet.isWhiteList();

        // Per Lark's record-filter-guide, CHECKBOX supports only the "is" operator - no "isNot" at all
        // (confirmed live: `field_checkbox != true` returned zero rows instead of the real 280 false rows).
        // A boolean blacklist excludes exactly one of its two possible values, so negate it and push "is"
        // with the opposite value instead of the unsupported "isNot".
        if (fieldUiType == UITypeEnum.CHECKBOX) {
            int valueCount = valueSet.getValueBlock().getRowCount();
            for (int i = 0; i < valueCount; i++) {
                Object value = valueSet.getValue(i);
                if (value instanceof Boolean booleanValue) {
                    boolean targetValue = isWhiteList == booleanValue;
                    conditions.add(createCondition(fieldName, "is", targetValue));
                }
            }
            return conditions;
        }

        String operator = isWhiteList ? "is" : "isNot";

        int valueCount = valueSet.getValueBlock().getRowCount();
        for (int i = 0; i < valueCount; i++) {
            Object value = valueSet.getValue(i);
            Object convertedValue = convertValueForSearchApi(value, fieldUiType);
            conditions.add(createCondition(fieldName, operator, convertedValue));
        }

        // A blacklist (NOT IN) that doesn't allow null is "col NOT IN (...)" with no "OR col IS NULL" - per
        // SQL's three-valued logic, a NULL column never satisfies "!=" (the comparison is UNKNOWN, not TRUE),
        // so NULL rows must NOT be in the result. But Lark's "isNot" operator treats an empty/unset field as
        // satisfying "isNot X" (it's trivially not equal to X), so without this, empty-field rows leak into
        // every NOT IN result. CHECKBOX has no "isNotEmpty" operator (see translateRangeSet's IS NOT NULL
        // handling) and Lark always returns a concrete true/false for it, so it's excluded here too.
        if (!isWhiteList && !valueSet.isNullAllowed() && fieldUiType != UITypeEnum.CHECKBOX) {
            conditions.add(createCondition(fieldName, "isNotEmpty", null));
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
            else if (value instanceof ExactDateValue exactDateValue) {
                // Confirmed against Lark's Search Records API directly: every comparison operator on a
                // DATE_TIME-family field (is/isNot/isGreater/isGreaterEqual/isLess/isLessEqual) requires a
                // TWO-element value array {"ExactDate", "<epoch millis>"} - a bare epoch-millis value is
                // rejected outright with "InvalidFilter ... not support this keyword". See
                // https://open.larksuite.com/document/.../record-filter-guide.
                condition.put("value", List.of("ExactDate", String.valueOf(exactDateValue.epochMillis())));
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

    /**
     * Marker wrapping a DATE_TIME-family value's epoch milliseconds so {@link #createCondition} can build
     * Lark's required {@code ["ExactDate", "<epoch millis>"]} two-element value array for it.
     */
    private record ExactDateValue(long epochMillis)
    {
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

        // DATE_TIME/CREATED_TIME/MODIFIED_TIME markers arrive as a java.time.LocalDateTime (the column's
        // Arrow type is Timestamp(MILLISECOND, "UTC")). Wrap the epoch millis in ExactDateValue so
        // createCondition can build Lark's required {"ExactDate", "<epoch millis>"} value array - a bare
        // value (whether LocalDateTime's ISO-8601 toString() or a plain millis number) is rejected outright,
        // so every date range/equality filter on these fields silently matched zero rows without this.
        if (isDateTimeUiType(fieldUiType) && value instanceof LocalDateTime localDateTime) {
            return new ExactDateValue(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        }

        return value;
    }

    private static boolean isDateTimeUiType(UITypeEnum uiType)
    {
        return uiType == UITypeEnum.DATE_TIME || uiType == UITypeEnum.CREATED_TIME || uiType == UITypeEnum.MODIFIED_TIME;
    }

    private static String convertToString(Object value)
    {
        if (value == null) {
            return "";
        }
        if (value instanceof Boolean) {
            return value.toString();
        }
        // BigDecimal.toString() switches to scientific notation for values with a small adjusted exponent
        // (e.g. a Decimal(38,18) zero prints as "0E-18"), which Lark's Search API cannot parse as a number,
        // silently dropping matching rows for equality/range filters on NUMBER/CURRENCY/PROGRESS/RATING
        // fields whose value is zero or otherwise near-zero. toPlainString() never uses scientific notation.
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
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
     * Creates conditions for: splitKey >= startIndex AND (endIndex == {@link Long#MAX_VALUE} ? true : splitKey <= endIndex)
     * <p>
     * {@code endIndex == Long.MAX_VALUE} means "no upper bound" - used for a split that must cover every
     * remaining row above {@code startIndex}, since {@code $reserved_split_key} is a user-populated
     * auto-number field whose value domain can have gaps or extend past the table's current row count
     * (e.g. after any row has ever been deleted - auto-number fields don't renumber or reclaim values).
     * {@code writeParallelPartitions} sizes splits off the row COUNT, which is only an accurate upper bound
     * on the key's value range for a table that has never had a row deleted; the last split is deliberately
     * left open-ended so rows with a higher key value than that estimate are never silently excluded from
     * every split's range.
     */
    public static String toSplitFilterJson(String existingFilterJson, long startIndex, long endIndex)
    {
        boolean isOpenEnded = endIndex == Long.MAX_VALUE;
        if (startIndex <= 0 || (endIndex <= 0 && !isOpenEnded)) {
            return existingFilterJson;
        }

        try {
            List<Map<String, Object>> allConditions = new ArrayList<>();
            List<Map<String, Object>> existingChildren = null;

            // Parse existing filter if present
            if (existingFilterJson != null && !existingFilterJson.isBlank()) {
                Map<String, Object> existingFilter = OBJECT_MAPPER.readValue(existingFilterJson, Map.class);
                List<Map<String, Object>> existingConditions = (List<Map<String, Object>>) existingFilter.get("conditions");
                if (existingConditions != null) {
                    allConditions.addAll(existingConditions);
                }
                // IN-clause conditions are carried as OR-groups under "children" (see toFilterJson); they must be
                // preserved here too, otherwise combining a split range with an IN-clause would silently drop it.
                existingChildren = (List<Map<String, Object>>) existingFilter.get("children");
            }

            // Add split range conditions
            Map<String, Object> startCondition = new HashMap<>();
            startCondition.put("field_name", RESERVED_SPLIT_KEY);
            startCondition.put("operator", "isGreaterEqual");
            startCondition.put("value", List.of(String.valueOf(startIndex)));
            allConditions.add(startCondition);

            if (!isOpenEnded) {
                Map<String, Object> endCondition = new HashMap<>();
                endCondition.put("field_name", RESERVED_SPLIT_KEY);
                endCondition.put("operator", "isLessEqual");
                endCondition.put("value", List.of(String.valueOf(endIndex)));
                allConditions.add(endCondition);
            }

            // Build combined filter
            Map<String, Object> filter = new HashMap<>();
            filter.put("conjunction", "and");
            filter.put("conditions", allConditions);
            if (existingChildren != null && !existingChildren.isEmpty()) {
                filter.put("children", existingChildren);
            }

            return OBJECT_MAPPER.writeValueAsString(filter);
        }
        catch (Exception e) {
            logger.error("Failed to build split filter JSON", e);
            return existingFilterJson;
        }
    }
}
