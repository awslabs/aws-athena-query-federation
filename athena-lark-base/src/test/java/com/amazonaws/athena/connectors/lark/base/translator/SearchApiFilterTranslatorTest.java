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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.lark.base.translator;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.amazonaws.athena.connectors.lark.base.model.AthenaFieldLarkBaseMapping;
import com.amazonaws.athena.connectors.lark.base.model.NestedUIType;
import com.amazonaws.athena.connectors.lark.base.model.enums.UITypeEnum;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SearchApiFilterTranslatorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testToFilterJson_nullConstraints_returnsEmptyString() {
        String filterJson = SearchApiFilterTranslator.toFilterJson(null, Collections.emptyList());
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_emptyConstraints_returnsEmptyString() {
        String filterJson = SearchApiFilterTranslator.toFilterJson(new HashMap<>(), Collections.emptyList());
        assertEquals("", filterJson);
    }

    // Note: Cannot test constraint creation in unit tests as AllOrNoneValueSet constructors are not public
    // Integration tests (regression tests) cover the actual filter translation

    @Test
    public void testToSortJson_nullOrderByFields_returnsEmptyString() {
        String sortJson = SearchApiFilterTranslator.toSortJson(null, Collections.emptyList());
        assertEquals("", sortJson);
    }

    @Test
    public void testToSortJson_emptyOrderByFields_returnsEmptyString() {
        String sortJson = SearchApiFilterTranslator.toSortJson(Collections.emptyList(), Collections.emptyList());
        assertEquals("", sortJson);
    }

    @Test
    public void testToSortJson_singleField_ascending() throws Exception {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField("field_number", OrderByField.Direction.ASC_NULLS_FIRST));

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "field_number",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        assertNotNull(sortJson);
        JsonNode sort = OBJECT_MAPPER.readTree(sortJson);
        assertEquals(1, sort.size());
        assertEquals("field_number", sort.get(0).get("field_name").asText());
        assertFalse(sort.get(0).get("desc").asBoolean());
    }

    @Test
    public void testToSortJson_singleField_descending() throws Exception {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField("field_number", OrderByField.Direction.DESC_NULLS_LAST));

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "field_number",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        assertNotNull(sortJson);
        JsonNode sort = OBJECT_MAPPER.readTree(sortJson);
        assertTrue(sort.get(0).get("desc").asBoolean());
    }

    @Test
    public void testToSortJson_multipleFields() throws Exception {
        List<OrderByField> orderByFields = Arrays.asList(
            new OrderByField("field_number", OrderByField.Direction.DESC_NULLS_LAST),
            new OrderByField("field_text", OrderByField.Direction.ASC_NULLS_FIRST));

        List<AthenaFieldLarkBaseMapping> mappings = Arrays.asList(
            new AthenaFieldLarkBaseMapping("field_number", "field_number",
                new NestedUIType(UITypeEnum.NUMBER, null)),
            new AthenaFieldLarkBaseMapping("field_text", "field_text",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        assertNotNull(sortJson);
        JsonNode sort = OBJECT_MAPPER.readTree(sortJson);
        assertEquals(2, sort.size());
        assertEquals("field_number", sort.get(0).get("field_name").asText());
        assertTrue(sort.get(0).get("desc").asBoolean());
        assertEquals("field_text", sort.get(1).get("field_name").asText());
        assertFalse(sort.get(1).get("desc").asBoolean());
    }

    @Test
    public void testToSplitFilterJson_withExistingFilter() throws Exception {
        String existingFilter = "{\"conjunction\":\"and\",\"conditions\":[{\"field_name\":\"field_number\",\"operator\":\"is\",\"value\":[\"123\"]}]}";

        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson(existingFilter, 1, 100);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        JsonNode conditions = filter.get("conditions");
        assertEquals(3, conditions.size()); // 1 existing + 2 split conditions

        boolean hasStartCondition = false;
        boolean hasEndCondition = false;
        for (JsonNode condition : conditions) {
            if ("$reserved_split_key".equals(condition.get("field_name").asText())) {
                String operator = condition.get("operator").asText();
                if ("isGreaterEqual".equals(operator)) {
                    hasStartCondition = true;
                    assertEquals("1", condition.get("value").get(0).asText());
                } else if ("isLessEqual".equals(operator)) {
                    hasEndCondition = true;
                    assertEquals("100", condition.get("value").get(0).asText());
                }
            }
        }
        assertTrue(hasStartCondition);
        assertTrue(hasEndCondition);
    }

    @Test
    public void testToSplitFilterJson_preservesChildrenOrGroup() throws Exception {
        // An IN-clause is carried as an OR-group under "children" (see toFilterJson). Combining it with a
        // parallel-split range must not silently drop that group, or the split's fetched rows would ignore
        // the query's IN-clause entirely.
        String existingFilter = "{\"conjunction\":\"and\",\"conditions\":[],"
                + "\"children\":[{\"conjunction\":\"or\",\"conditions\":["
                + "{\"field_name\":\"status\",\"operator\":\"is\",\"value\":[\"active\"]},"
                + "{\"field_name\":\"status\",\"operator\":\"is\",\"value\":[\"pending\"]}]}]}";

        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson(existingFilter, 1, 100);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        assertEquals(2, filter.get("conditions").size()); // just the 2 split-range conditions

        JsonNode children = filter.get("children");
        assertNotNull(children);
        assertEquals(1, children.size());
        assertEquals("or", children.get(0).get("conjunction").asText());
        assertEquals(2, children.get(0).get("conditions").size());
    }

    @Test
    public void testToSplitFilterJson_noExistingFilter() throws Exception {
        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson(null, 1, 100);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size()); // Only split conditions

        assertEquals("$reserved_split_key", conditions.get(0).get("field_name").asText());
        assertEquals("isGreaterEqual", conditions.get(0).get("operator").asText());
        assertEquals("1", conditions.get(0).get("value").get(0).asText());

        assertEquals("$reserved_split_key", conditions.get(1).get("field_name").asText());
        assertEquals("isLessEqual", conditions.get(1).get("operator").asText());
        assertEquals("100", conditions.get(1).get("value").get(0).asText());
    }

    @Test
    public void testToSplitFilterJson_invalidRange_returnsExisting() {
        String existingFilter = "{\"conjunction\":\"and\",\"conditions\":[]}";

        String result = SearchApiFilterTranslator.toSplitFilterJson(existingFilter, 0, 0);
        assertEquals(existingFilter, result);

        result = SearchApiFilterTranslator.toSplitFilterJson(existingFilter, -1, 100);
        assertEquals(existingFilter, result);

        result = SearchApiFilterTranslator.toSplitFilterJson(existingFilter, 100, -1);
        assertEquals(existingFilter, result);
    }

    @Test
    public void testToSplitFilterJson_blankExistingFilter() throws Exception {
        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson("", 1, 100);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size()); // Only split conditions, no existing
    }

    // ========== Tests for toFilterJson with different ValueSet types ==========

    @Test
    public void testToFilterJson_withSortedRangeSet_singleValue() throws Exception {
        // Mock SortedRangeSet with single value
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(123);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        assertFalse(filterJson.isEmpty());
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        assertEquals("and", filter.get("conjunction").asText());
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Number Field", conditions.get(0).get("field_name").asText());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("123", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_singleValue_checkbox_null() throws Exception {
        // Mock SortedRangeSet with null value for checkbox (should convert to false)
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(null);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Bool());

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Checkbox Field", conditions.get(0).get("field_name").asText());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("false", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_singleValue_nonCheckbox_null() throws Exception {
        // A pure "IS NULL" constraint on a non-checkbox column arrives as a SortedRangeSet with zero ranges
        // and nullAllowed=true, which makes isSingleValue() true with getSingleValue() == null. This must be
        // pushed down as "isEmpty", not "is ''" (which Lark's Search API treats as equals-empty-string and
        // matches zero rows instead of the actual NULL rows).
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(null);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_single_select", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_single_select", "Single Select Field",
                new NestedUIType(UITypeEnum.SINGLE_SELECT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Single Select Field", conditions.get(0).get("field_name").asText());
        assertEquals("isEmpty", conditions.get(0).get("operator").asText());
        assertEquals(0, conditions.get(0).get("value").size());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_isNotNull_checkbox() throws Exception {
        // Mock SortedRangeSet for IS NOT NULL pattern with checkbox
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Bool());

        // Mock for isEffectivelyNotNull - single range with null bounds
        Ranges ranges = mock(Ranges.class);
        Range range = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);

        when(lowMarker.isNullValue()).thenReturn(true);
        when(lowMarker.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(highMarker.isNullValue()).thenReturn(true);
        when(highMarker.getBound()).thenReturn(Marker.Bound.BELOW);
        when(range.getLow()).thenReturn(lowMarker);
        when(range.getHigh()).thenReturn(highMarker);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(range));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Regression test: CHECKBOX has no separate empty state in Lark - every row is genuinely true
        // or false - so "IS NOT NULL" is a tautology that matches every row, not "equals true". This
        // used to push `is true`, which silently excluded every `false` row from the result. No
        // condition should be pushed at all; Athena's own engine applies the (always-true) check
        // itself against the real materialized value.
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_isNotNull_nonCheckbox() throws Exception {
        // Mock SortedRangeSet for IS NOT NULL pattern with non-checkbox
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        // Mock span for unbounded range
        Range span = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);
        when(lowMarker.isLowerUnbounded()).thenReturn(true);
        when(highMarker.isUpperUnbounded()).thenReturn(true);
        when(span.getLow()).thenReturn(lowMarker);
        when(span.getHigh()).thenReturn(highMarker);
        when(valueSet.getSpan()).thenReturn(span);

        Ranges ranges = mock(Ranges.class);
        when(ranges.getOrderedRanges()).thenReturn(Collections.emptyList());
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Text Field", conditions.get(0).get("field_name").asText());
        assertEquals("isNotEmpty", conditions.get(0).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_ranges_greaterThan() throws Exception {
        // Mock SortedRangeSet with > condition
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);
        Range range = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);

        when(lowMarker.isLowerUnbounded()).thenReturn(false);
        when(lowMarker.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(lowMarker.getValue()).thenReturn(10);
        when(highMarker.isUpperUnbounded()).thenReturn(true);
        when(range.getLow()).thenReturn(lowMarker);
        when(range.getHigh()).thenReturn(highMarker);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(range));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Number Field", conditions.get(0).get("field_name").asText());
        assertEquals("isGreater", conditions.get(0).get("operator").asText());
        assertEquals("10", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_ranges_greaterThanOrEqual() throws Exception {
        // Mock SortedRangeSet with >= condition
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);
        Range range = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);

        when(lowMarker.isLowerUnbounded()).thenReturn(false);
        when(lowMarker.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(lowMarker.getValue()).thenReturn(10);
        when(highMarker.isUpperUnbounded()).thenReturn(true);
        when(range.getLow()).thenReturn(lowMarker);
        when(range.getHigh()).thenReturn(highMarker);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(range));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("isGreaterEqual", conditions.get(0).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_ranges_lessThan() throws Exception {
        // Mock SortedRangeSet with < condition
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);
        Range range = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);

        when(lowMarker.isLowerUnbounded()).thenReturn(true);
        when(highMarker.isUpperUnbounded()).thenReturn(false);
        when(highMarker.getBound()).thenReturn(Marker.Bound.BELOW);
        when(highMarker.getValue()).thenReturn(100);
        when(range.getLow()).thenReturn(lowMarker);
        when(range.getHigh()).thenReturn(highMarker);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(range));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("isLess", conditions.get(0).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_ranges_lessThanOrEqual() throws Exception {
        // Mock SortedRangeSet with <= condition
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);
        Range range = mock(Range.class);
        Marker lowMarker = mock(Marker.class);
        Marker highMarker = mock(Marker.class);

        when(lowMarker.isLowerUnbounded()).thenReturn(true);
        when(highMarker.isUpperUnbounded()).thenReturn(false);
        when(highMarker.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(highMarker.getValue()).thenReturn(100);
        when(range.getLow()).thenReturn(lowMarker);
        when(range.getHigh()).thenReturn(highMarker);
        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(range));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("isLessEqual", conditions.get(0).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_ranges_exception() throws Exception {
        // Mock SortedRangeSet that throws exception when getting ranges
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));
        when(valueSet.getRanges()).thenThrow(new RuntimeException("Range error"));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Should return empty filter due to exception
        assertNotNull(filterJson);
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_multiRangeUnion_pushedDownAsOrGroup() throws Exception {
        // WHERE field_number < 5 OR field_number > 100 - Presto/Trino models this as a single SortedRangeSet
        // with two disjoint, single-bounded ranges (per the SDK's own definition of SortedRangeSet: "col
        // between 10 and 30, or col between 40 and 60, ..."). Flattening both ranges' conditions into the
        // same top-level AND list (the pre-fix behavior) would produce "field_number < 5 AND field_number >
        // 100", which can never match anything. It must instead become an OR-group.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);

        Range lessThanFive = mock(Range.class);
        Marker lessThanFiveLow = mock(Marker.class);
        Marker lessThanFiveHigh = mock(Marker.class);
        when(lessThanFiveLow.isLowerUnbounded()).thenReturn(true);
        when(lessThanFiveHigh.isUpperUnbounded()).thenReturn(false);
        when(lessThanFiveHigh.getBound()).thenReturn(Marker.Bound.BELOW);
        when(lessThanFiveHigh.getValue()).thenReturn(5);
        when(lessThanFive.getLow()).thenReturn(lessThanFiveLow);
        when(lessThanFive.getHigh()).thenReturn(lessThanFiveHigh);

        Range greaterThanHundred = mock(Range.class);
        Marker greaterThanHundredLow = mock(Marker.class);
        Marker greaterThanHundredHigh = mock(Marker.class);
        when(greaterThanHundredLow.isLowerUnbounded()).thenReturn(false);
        when(greaterThanHundredLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(greaterThanHundredLow.getValue()).thenReturn(100);
        when(greaterThanHundredHigh.isUpperUnbounded()).thenReturn(true);
        when(greaterThanHundred.getLow()).thenReturn(greaterThanHundredLow);
        when(greaterThanHundred.getHigh()).thenReturn(greaterThanHundredHigh);

        when(ranges.getOrderedRanges()).thenReturn(Arrays.asList(lessThanFive, greaterThanHundred));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        // No top-level AND conditions for this field - it must live entirely inside the OR-group.
        assertEquals(0, filter.get("conditions").size());

        JsonNode children = filter.get("children");
        assertEquals(1, children.size());
        JsonNode orGroup = children.get(0);
        assertEquals("or", orGroup.get("conjunction").asText());
        JsonNode orConditions = orGroup.get("conditions");
        assertEquals(2, orConditions.size());
        assertEquals("isLess", orConditions.get(0).get("operator").asText());
        assertEquals("5", orConditions.get(0).get("value").get(0).asText());
        assertEquals("isGreater", orConditions.get(1).get("operator").asText());
        assertEquals("100", orConditions.get(1).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_notEqualPattern_singleSelect_pushesAsIsNot() throws Exception {
        // WHERE field_single_select != 'Option A' (or NOT IN with one value) - a column with a natural
        // ordering represents this as two ranges excluding the single point "Option A": (-inf, 'Option A')
        // union ('Option A', +inf), same shape as the range-union case above but with identical boundary
        // values on both ranges. Routing this through the generic range-union path would emit
        // isLess/isGreater, which Lark's Search API doesn't support for a categorical SINGLE_SELECT field
        // (it has no meaningful ordering there) and silently matches zero rows. It must become "isNot"
        // instead, which works for every equality-capable type.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Ranges ranges = mock(Ranges.class);

        Range belowOptionA = mock(Range.class);
        Marker belowLow = mock(Marker.class);
        Marker belowHigh = mock(Marker.class);
        when(belowLow.isLowerUnbounded()).thenReturn(true);
        when(belowHigh.isUpperUnbounded()).thenReturn(false);
        when(belowHigh.getBound()).thenReturn(Marker.Bound.BELOW);
        when(belowHigh.getValue()).thenReturn("Option A");
        when(belowOptionA.getLow()).thenReturn(belowLow);
        when(belowOptionA.getHigh()).thenReturn(belowHigh);

        Range aboveOptionA = mock(Range.class);
        Marker aboveLow = mock(Marker.class);
        Marker aboveHigh = mock(Marker.class);
        when(aboveLow.isLowerUnbounded()).thenReturn(false);
        when(aboveLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(aboveLow.getValue()).thenReturn("Option A");
        when(aboveHigh.isUpperUnbounded()).thenReturn(true);
        when(aboveOptionA.getLow()).thenReturn(aboveLow);
        when(aboveOptionA.getHigh()).thenReturn(aboveHigh);

        when(ranges.getOrderedRanges()).thenReturn(Arrays.asList(belowOptionA, aboveOptionA));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_single_select", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_single_select", "Single Select Field",
                new NestedUIType(UITypeEnum.SINGLE_SELECT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        // Must be a flat top-level condition, not an OR-group under "children".
        assertNull(filter.get("children"));
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size());
        assertEquals("isNot", conditions.get(0).get("operator").asText());
        assertEquals("Option A", conditions.get(0).get("value").get(0).asText());
        assertEquals("isNotEmpty", conditions.get(1).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_notInPattern_multipleValues_pushesAsMultipleIsNot() throws Exception {
        // WHERE field_text NOT IN ('a', 'b') on an orderable type is modeled as THREE ranges excluding two
        // points: (-inf, 'a') union ('a', 'b') union ('b', +inf) - the middle range needs both bounds, so
        // buildRangeUnionOrGroup's single-bound requirement would reject it and skip pushdown entirely
        // (falling back to an unfiltered fetch). tryGetExcludedValues must recognize this N-point shape too,
        // not just the single-point "!=" case, and emit one "isNot" per excluded value.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Ranges ranges = mock(Ranges.class);

        Range belowA = mock(Range.class);
        Marker belowALow = mock(Marker.class);
        Marker belowAHigh = mock(Marker.class);
        when(belowALow.isLowerUnbounded()).thenReturn(true);
        when(belowAHigh.isUpperUnbounded()).thenReturn(false);
        when(belowAHigh.getBound()).thenReturn(Marker.Bound.BELOW);
        when(belowAHigh.getValue()).thenReturn("a");
        when(belowA.getLow()).thenReturn(belowALow);
        when(belowA.getHigh()).thenReturn(belowAHigh);

        Range betweenAAndB = mock(Range.class);
        Marker betweenLow = mock(Marker.class);
        Marker betweenHigh = mock(Marker.class);
        when(betweenLow.isLowerUnbounded()).thenReturn(false);
        when(betweenLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(betweenLow.getValue()).thenReturn("a");
        when(betweenHigh.isUpperUnbounded()).thenReturn(false);
        when(betweenHigh.getBound()).thenReturn(Marker.Bound.BELOW);
        when(betweenHigh.getValue()).thenReturn("b");
        when(betweenAAndB.getLow()).thenReturn(betweenLow);
        when(betweenAAndB.getHigh()).thenReturn(betweenHigh);

        Range aboveB = mock(Range.class);
        Marker aboveBLow = mock(Marker.class);
        Marker aboveBHigh = mock(Marker.class);
        when(aboveBLow.isLowerUnbounded()).thenReturn(false);
        when(aboveBLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(aboveBLow.getValue()).thenReturn("b");
        when(aboveBHigh.isUpperUnbounded()).thenReturn(true);
        when(aboveB.getLow()).thenReturn(aboveBLow);
        when(aboveB.getHigh()).thenReturn(aboveBHigh);

        when(ranges.getOrderedRanges()).thenReturn(Arrays.asList(belowA, betweenAAndB, aboveB));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        assertNull(filter.get("children"));
        JsonNode conditions = filter.get("conditions");
        assertEquals(3, conditions.size());
        assertEquals("isNot", conditions.get(0).get("operator").asText());
        assertEquals("a", conditions.get(0).get("value").get(0).asText());
        assertEquals("isNot", conditions.get(1).get("operator").asText());
        assertEquals("b", conditions.get(1).get("value").get(0).asText());
        assertEquals("isNotEmpty", conditions.get(2).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_multiRangeUnionWithDoubleBoundedRange_skipsPushdown() throws Exception {
        // A union containing a range that needs BOTH bounds (e.g. one BETWEEN-shaped range OR'd with a
        // single-bounded range) can't be expressed within Lark's one-level-of-nesting filter API (it would
        // require "OR of ANDs"). It must be skipped entirely rather than pushed down incorrectly.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Int(32, true));

        Ranges ranges = mock(Ranges.class);

        // First range: BETWEEN 10 AND 20 (both bounds set).
        Range between = mock(Range.class);
        Marker betweenLow = mock(Marker.class);
        Marker betweenHigh = mock(Marker.class);
        when(betweenLow.isLowerUnbounded()).thenReturn(false);
        when(betweenLow.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(betweenLow.getValue()).thenReturn(10);
        when(betweenHigh.isUpperUnbounded()).thenReturn(false);
        when(betweenHigh.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(betweenHigh.getValue()).thenReturn(20);
        when(between.getLow()).thenReturn(betweenLow);
        when(between.getHigh()).thenReturn(betweenHigh);

        // Second range: > 100 (single-bounded).
        Range greaterThanHundred = mock(Range.class);
        Marker greaterThanHundredLow = mock(Marker.class);
        Marker greaterThanHundredHigh = mock(Marker.class);
        when(greaterThanHundredLow.isLowerUnbounded()).thenReturn(false);
        when(greaterThanHundredLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(greaterThanHundredLow.getValue()).thenReturn(100);
        when(greaterThanHundredHigh.isUpperUnbounded()).thenReturn(true);
        when(greaterThanHundred.getLow()).thenReturn(greaterThanHundredLow);
        when(greaterThanHundred.getHigh()).thenReturn(greaterThanHundredHigh);

        when(ranges.getOrderedRanges()).thenReturn(Arrays.asList(between, greaterThanHundred));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Not pushed down at all - correctness over optimization when it can't be expressed safely.
        assertNotNull(filterJson);
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_whitelist() throws Exception {
        // Mock EquatableValueSet with whitelist (IN clause)
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(true);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(2);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn("value1");
        when(valueSet.getValue(1)).thenReturn("value2");

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);

        // Lark's "is" operator only accepts a single value, so an IN-clause with multiple values must NOT be
        // emitted as multiple top-level "is" conditions ANDed together (that would require the field to equal
        // both values at once and always match zero rows). It must be an OR-group nested under "children".
        JsonNode conditions = filter.get("conditions");
        assertEquals(0, conditions.size());

        JsonNode children = filter.get("children");
        assertEquals(1, children.size());
        JsonNode orGroup = children.get(0);
        assertEquals("or", orGroup.get("conjunction").asText());
        JsonNode orConditions = orGroup.get("conditions");
        assertEquals(2, orConditions.size());
        assertEquals("Text Field", orConditions.get(0).get("field_name").asText());
        assertEquals("is", orConditions.get(0).get("operator").asText());
        assertEquals("value1", orConditions.get(0).get("value").get(0).asText());
        assertEquals("is", orConditions.get(1).get("operator").asText());
        assertEquals("value2", orConditions.get(1).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_whitelistSingleValue_staysFlatCondition() throws Exception {
        // A single-value IN-clause (effectively "=") should stay a plain top-level condition, not a "children" group.
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(true);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(1);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn("value1");

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertNull(filter.get("children"));
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_multiValueInClauseCombinedWithOtherColumn() throws Exception {
        // WHERE status IN ('active', 'pending') AND priority = 'high'
        EquatableValueSet inClauseValueSet = mock(EquatableValueSet.class);
        when(inClauseValueSet.isWhiteList()).thenReturn(true);
        when(inClauseValueSet.isNullAllowed()).thenReturn(false);
        when(inClauseValueSet.getType()).thenReturn(new ArrowType.Utf8());
        Block inClauseBlock = mock(Block.class);
        when(inClauseBlock.getRowCount()).thenReturn(2);
        when(inClauseValueSet.getValueBlock()).thenReturn(inClauseBlock);
        when(inClauseValueSet.getValue(0)).thenReturn("active");
        when(inClauseValueSet.getValue(1)).thenReturn("pending");

        EquatableValueSet equalityValueSet = mock(EquatableValueSet.class);
        when(equalityValueSet.isWhiteList()).thenReturn(true);
        when(equalityValueSet.isNullAllowed()).thenReturn(false);
        when(equalityValueSet.getType()).thenReturn(new ArrowType.Utf8());
        Block equalityBlock = mock(Block.class);
        when(equalityBlock.getRowCount()).thenReturn(1);
        when(equalityValueSet.getValueBlock()).thenReturn(equalityBlock);
        when(equalityValueSet.getValue(0)).thenReturn("high");

        Map<String, ValueSet> constraints = new LinkedHashMap<>();
        constraints.put("status", inClauseValueSet);
        constraints.put("priority", equalityValueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Arrays.asList(
            new AthenaFieldLarkBaseMapping("status", "Status", new NestedUIType(UITypeEnum.TEXT, null)),
            new AthenaFieldLarkBaseMapping("priority", "Priority", new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        assertEquals("and", filter.get("conjunction").asText());

        // The single-value equality condition stays a flat, top-level AND condition.
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Priority", conditions.get(0).get("field_name").asText());

        // The multi-value IN-clause becomes its own OR-group.
        JsonNode children = filter.get("children");
        assertEquals(1, children.size());
        JsonNode orGroup = children.get(0);
        assertEquals("or", orGroup.get("conjunction").asText());
        assertEquals(2, orGroup.get("conditions").size());
        assertEquals("Status", orGroup.get("conditions").get(0).get("field_name").asText());
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_blacklist() throws Exception {
        // Mock EquatableValueSet with blacklist (NOT IN clause)
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(1);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn("excluded");

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        // A second "isNotEmpty" condition excludes NULL rows: per SQL's three-valued logic a NULL column
        // never satisfies "!=", but Lark's "isNot" alone would let an empty field leak into the result.
        assertEquals(2, conditions.size());
        assertEquals("isNot", conditions.get(0).get("operator").asText());
        assertEquals("excluded", conditions.get(0).get("value").get(0).asText());
        assertEquals("isNotEmpty", conditions.get(1).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_multiValueBlacklist_staysFlatAndCondition() throws Exception {
        // WHERE field NOT IN ('a', 'b') is correctly "field != a AND field != b" - unlike the whitelist (IN) case,
        // ANDing multiple "isNot" conditions together is already correct, so this must stay flat, not grouped.
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(2);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn("a");
        when(valueSet.getValue(1)).thenReturn("b");

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        // Plus the trailing "isNotEmpty" that excludes NULL rows (see the blacklist single-value test).
        assertEquals(3, conditions.size());
        assertEquals("isNot", conditions.get(0).get("operator").asText());
        assertEquals("isNot", conditions.get(1).get("operator").asText());
        assertEquals("isNotEmpty", conditions.get(2).get("operator").asText());
        assertNull(filter.get("children"));
    }

    @Test
    public void testToFilterJson_withAllOrNoneValueSet_isNull() throws Exception {
        // AllOrNoneValueSet with all=false, nullAllowed=true (IS NULL for non-checkbox)
        AllOrNoneValueSet valueSet = new AllOrNoneValueSet(new ArrowType.Utf8(), false, true);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Text Field", conditions.get(0).get("field_name").asText());
        assertEquals("isEmpty", conditions.get(0).get("operator").asText());
        assertTrue(conditions.get(0).get("value").isEmpty());
    }

    @Test
    public void testToFilterJson_withAllOrNoneValueSet_isNull_checkbox_skipped() throws Exception {
        // AllOrNoneValueSet with all=false, nullAllowed=true for checkbox (should be skipped)
        AllOrNoneValueSet valueSet = new AllOrNoneValueSet(new ArrowType.Bool(), false, true);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // For checkbox with isAll=false, no condition should be added
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_noMappingFound() throws Exception {
        // Test when no mapping is found for a constraint
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(123);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("unknown_field", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Should return empty string when no mapping found
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_unsupportedUIType() throws Exception {
        // Test with unsupported UI type (should be skipped)
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn("test");

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_attachment", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_attachment", "Attachment Field",
                new NestedUIType(UITypeEnum.ATTACHMENT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Should return empty string for unsupported UI type
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_supportedUITypes() throws Exception {
        // Test all supported UI types
        UITypeEnum[] supportedTypes = {
            UITypeEnum.TEXT, UITypeEnum.BARCODE, UITypeEnum.SINGLE_SELECT,
            UITypeEnum.PHONE, UITypeEnum.NUMBER, UITypeEnum.PROGRESS,
            UITypeEnum.CURRENCY, UITypeEnum.RATING, UITypeEnum.CHECKBOX,
            UITypeEnum.EMAIL, UITypeEnum.DATE_TIME, UITypeEnum.CREATED_TIME,
            UITypeEnum.MODIFIED_TIME
        };

        for (UITypeEnum uiType : supportedTypes) {
            SortedRangeSet valueSet = mock(SortedRangeSet.class);
            when(valueSet.isSingleValue()).thenReturn(true);
            when(valueSet.getSingleValue()).thenReturn(uiType == UITypeEnum.CHECKBOX ? true : "test");

            Map<String, ValueSet> constraints = new HashMap<>();
            constraints.put("test_field", valueSet);

            List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
                new AthenaFieldLarkBaseMapping("test_field", "Test Field",
                    new NestedUIType(uiType, null)));

            String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

            // All supported types should produce non-empty filter
            assertFalse("UI type " + uiType + " should be supported", filterJson.isEmpty());
        }
    }

    // ========== Tests for toSortJson edge cases ==========

    @Test
    public void testToSortJson_nullColumnName() throws Exception {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField(null, OrderByField.Direction.ASC_NULLS_FIRST));

        List<AthenaFieldLarkBaseMapping> mappings = Collections.emptyList();

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        // Should return empty string for null column name
        assertEquals("", sortJson);
    }

    @Test
    public void testToSortJson_emptyColumnName() throws Exception {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField("", OrderByField.Direction.ASC_NULLS_FIRST));

        List<AthenaFieldLarkBaseMapping> mappings = Collections.emptyList();

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        // Should return empty string for empty column name
        assertEquals("", sortJson);
    }

    @Test
    public void testToSortJson_nullMappings() throws Exception {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField("field_name", OrderByField.Direction.ASC_NULLS_FIRST));

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, null);

        // Should use column name as-is when mappings is null
        assertNotNull(sortJson);
        JsonNode sort = OBJECT_MAPPER.readTree(sortJson);
        assertEquals("field_name", sort.get(0).get("field_name").asText());
    }

    // ========== Tests for toSplitFilterJson edge cases ==========

    @Test
    public void testToSplitFilterJson_invalidJson() throws Exception {
        String invalidJson = "{invalid json}";

        String result = SearchApiFilterTranslator.toSplitFilterJson(invalidJson, 1, 100);

        // Should return original filter on parse error
        assertEquals(invalidJson, result);
    }

    @Test
    public void testToSplitFilterJson_existingFilterWithoutConditions() throws Exception {
        String filterWithoutConditions = "{\"conjunction\":\"and\"}";

        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson(filterWithoutConditions, 1, 100);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size()); // Only split conditions added
    }

    @Test
    public void testToSplitFilterJson_zeroStartIndex() {
        String existing = "{\"conjunction\":\"and\",\"conditions\":[]}";
        String result = SearchApiFilterTranslator.toSplitFilterJson(existing, 0, 100);
        assertEquals(existing, result);
    }

    @Test
    public void testToSplitFilterJson_zeroEndIndex() {
        String existing = "{\"conjunction\":\"and\",\"conditions\":[]}";
        String result = SearchApiFilterTranslator.toSplitFilterJson(existing, 100, 0);
        assertEquals(existing, result);
    }

    @Test
    public void testToSplitFilterJson_openEndedMaxValue_omitsUpperBound() throws Exception {
        // Long.MAX_VALUE as endIndex signals "no upper bound" (used for the last parallel split, since
        // $reserved_split_key can have gaps/exceed the row-count estimate after any row deletion) - only
        // the lower-bound condition should be pushed, not a literal "isLessEqual 9223372036854775807".
        String splitFilter = SearchApiFilterTranslator.toSplitFilterJson(null, 501, Long.MAX_VALUE);

        assertNotNull(splitFilter);
        JsonNode filter = OBJECT_MAPPER.readTree(splitFilter);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("$reserved_split_key", conditions.get(0).get("field_name").asText());
        assertEquals("isGreaterEqual", conditions.get(0).get("operator").asText());
        assertEquals("501", conditions.get(0).get("value").get(0).asText());
    }

    // ========== Additional edge case tests ==========

    @Test
    public void testToFilterJson_nullMappingsParameter() throws Exception {
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(123);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, null);

        // Should return empty string when mappings is null
        assertEquals("", filterJson);
    }

    @Test
    public void testCreateCondition_withIsEmpty_andNullValue() throws Exception {
        // Test isEmpty operator with null value (should use empty list)
        AllOrNoneValueSet valueSet = new AllOrNoneValueSet(new ArrowType.Utf8(), false, true);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals("isEmpty", conditions.get(0).get("operator").asText());
        // Value should be empty array for isEmpty
        assertTrue(conditions.get(0).get("value").isEmpty());
    }

    @Test
    public void testConvertToString_withNull() throws Exception {
        // Test convertToString with null value by using null in value
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(true);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(1);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn(null);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        // Null value should be converted to empty string
        assertEquals("", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testConvertValueForSearchApi_withNullValue() throws Exception {
        // Test convertValueForSearchApi with null value
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(null);
        when(valueSet.isNullAllowed()).thenReturn(true);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        // A single-value domain of null is a pure IS NULL constraint; for non-checkbox fields this must be
        // "isEmpty", not "is ''" (which Lark's Search API treats as equals-empty-string, matching zero rows
        // instead of the actual NULL rows - see testToFilterJson_withSortedRangeSet_singleValue_nonCheckbox_null).
        assertEquals("isEmpty", conditions.get(0).get("operator").asText());
        assertEquals(0, conditions.get(0).get("value").size());
    }

    @Test
    public void testIsEffectivelyNotNull_exceptionHandling() throws Exception {
        // Test isEffectivelyNotNull with exception in getSpan()
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Ranges ranges = mock(Ranges.class);
        when(ranges.getOrderedRanges()).thenReturn(Collections.emptyList());
        when(valueSet.getRanges()).thenReturn(ranges);
        when(valueSet.getSpan()).thenThrow(new RuntimeException("Span error"));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // Should return empty due to exception (returns empty string when conditions list is empty)
        assertEquals("", filterJson);
    }

    @Test
    public void testFindMappingForColumn_withNullMappings() throws Exception {
        // Test with null mappings parameter
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(123);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, null);

        // Should return empty string when no mapping found
        assertEquals("", filterJson);
    }

    @Test
    public void testConvertToString_withBoolean() throws Exception {
        // Test convertToString with Boolean value
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(true);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Bool());

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals("true", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testConvertToString_withNumber() throws Exception {
        // Test convertToString with Number value
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(42.5);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_number", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_number", "Number Field",
                new NestedUIType(UITypeEnum.NUMBER, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals("42.5", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testConvertToString_withBigDecimalZero_avoidsScientificNotation() throws Exception {
        // Decimal(38, 18) columns (NUMBER/CURRENCY/PROGRESS) hand a BigDecimal with scale 18 to the
        // translator. BigDecimal.toString() renders a zero at that scale as "0E-18" (scientific notation),
        // which Lark's Search API can't parse as a number - it must come out as a plain "0".
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(new java.math.BigDecimal("0.000000000000000000"));
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Decimal(38, 18, 128));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_currency", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_currency", "Currency Field",
                new NestedUIType(UITypeEnum.CURRENCY, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        // toPlainString() keeps the scale (unlike toString()'s "0E-18"), which is fine - Lark parses a
        // plain decimal string regardless of trailing zeros; the point is it must never be scientific notation.
        assertEquals("0.000000000000000000", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withDateTimeValue_convertsToExactDateEpochMillis() throws Exception {
        // DATE_TIME/CREATED_TIME/MODIFIED_TIME markers arrive as java.time.LocalDateTime. Confirmed directly
        // against Lark's Search Records API: a bare epoch-millis value ("1735689600000") is rejected
        // outright ("InvalidFilter ... not support this keyword"), and LocalDateTime.toString()
        // ("2025-01-01T00:00") fails the same way. Every comparison operator on a date field requires the
        // two-element value array {"ExactDate", "<epoch millis>"}.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(true);
        when(valueSet.getSingleValue()).thenReturn(java.time.LocalDateTime.of(2025, 1, 1, 0, 0, 0));
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"));

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_date_time", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_date_time", "Date Time Field",
                new NestedUIType(UITypeEnum.DATE_TIME, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        JsonNode value = conditions.get(0).get("value");
        assertEquals(2, value.size());
        assertEquals("ExactDate", value.get(0).asText());
        assertEquals("1735689600000", value.get(1).asText());
    }

    @Test
    public void testToFilterJson_withDateTimeBetween_usesStrictOperators() throws Exception {
        // WHERE field_date_time BETWEEN t1 AND t2 has both bounds Marker.Bound.EXACTLY (normally inclusive,
        // which addRangeBoundConditions maps to isGreaterEqual/isLessEqual). Confirmed directly against
        // Lark's Search Records API: a DATE_TIME-family field rejects those outright ("fieldType '5' not
        // support isGreaterEqual"). It must fall back to the strict isGreater/isLess instead - the exact
        // boundary instant won't match, an accepted platform limitation.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, "UTC"));

        Ranges ranges = mock(Ranges.class);
        Range between = mock(Range.class);
        Marker low = mock(Marker.class);
        Marker high = mock(Marker.class);
        when(low.isLowerUnbounded()).thenReturn(false);
        when(low.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(low.getValue()).thenReturn(java.time.LocalDateTime.of(1990, 1, 1, 0, 0, 0));
        when(high.isUpperUnbounded()).thenReturn(false);
        when(high.getBound()).thenReturn(Marker.Bound.EXACTLY);
        when(high.getValue()).thenReturn(java.time.LocalDateTime.of(2000, 1, 1, 0, 0, 0));
        when(between.getLow()).thenReturn(low);
        when(between.getHigh()).thenReturn(high);

        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(between));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_date_time", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_date_time", "Date Time Field",
                new NestedUIType(UITypeEnum.DATE_TIME, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size());
        assertEquals("isGreater", conditions.get(0).get("operator").asText());
        assertEquals("isLess", conditions.get(1).get("operator").asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_range_nonOrderableType_skipsPushdown() throws Exception {
        // Per Lark's record-filter-guide, TEXT/BARCODE/PHONE/EMAIL/SINGLE_SELECT have NO ordering operators
        // at all (only is/isNot/contains/doesNotContain/isEmpty/isNotEmpty) - confirmed live:
        // `field_text > 'M'` and `field_single_select > 'Option A'` both returned zero rows instead of the
        // real match counts. A genuine range constraint on one of these types must be skipped entirely
        // (falling back to client-side filtering), not pushed down as an unsupported isGreater/isLess.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Utf8());

        Ranges ranges = mock(Ranges.class);
        Range greaterThanM = mock(Range.class);
        Marker low = mock(Marker.class);
        Marker high = mock(Marker.class);
        when(low.isLowerUnbounded()).thenReturn(false);
        when(low.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(low.getValue()).thenReturn("M");
        when(high.isUpperUnbounded()).thenReturn(true);
        when(greaterThanM.getLow()).thenReturn(low);
        when(greaterThanM.getHigh()).thenReturn(high);

        when(ranges.getOrderedRanges()).thenReturn(Collections.singletonList(greaterThanM));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_text", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_text", "Text Field",
                new NestedUIType(UITypeEnum.TEXT, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        // No condition pushed for this field at all - not isGreater, not anything.
        assertEquals("", filterJson);
    }

    @Test
    public void testToFilterJson_withEquatableValueSet_checkbox_blacklist_negatesToIs() throws Exception {
        // Per Lark's record-filter-guide, CHECKBOX supports only "is" - no "isNot" at all (confirmed live:
        // `field_checkbox != true` returned zero rows instead of the real 280 false rows). A boolean
        // blacklist must negate the excluded value and push "is" with the opposite instead.
        EquatableValueSet valueSet = mock(EquatableValueSet.class);
        when(valueSet.isWhiteList()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Bool());

        Block block = mock(Block.class);
        when(block.getRowCount()).thenReturn(1);
        when(valueSet.getValueBlock()).thenReturn(block);
        when(valueSet.getValue(0)).thenReturn(true);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("false", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testToFilterJson_withSortedRangeSet_checkbox_notEqualPattern_negatesToIs() throws Exception {
        // Boolean has an ordering (false < true) in Presto/Trino, so "field_checkbox != true" reaches
        // toFilterJson's SortedRangeSet "!=" detection too, not just the EquatableValueSet blacklist path
        // covered by testToFilterJson_withEquatableValueSet_checkbox_blacklist_negatesToIs. Confirmed live:
        // without this, it pushed the unsupported "isNot" and returned zero rows instead of the real 280.
        SortedRangeSet valueSet = mock(SortedRangeSet.class);
        when(valueSet.isSingleValue()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getType()).thenReturn(new ArrowType.Bool());

        Ranges ranges = mock(Ranges.class);

        Range belowTrue = mock(Range.class);
        Marker belowLow = mock(Marker.class);
        Marker belowHigh = mock(Marker.class);
        when(belowLow.isLowerUnbounded()).thenReturn(true);
        when(belowHigh.isUpperUnbounded()).thenReturn(false);
        when(belowHigh.getBound()).thenReturn(Marker.Bound.BELOW);
        when(belowHigh.getValue()).thenReturn(true);
        when(belowTrue.getLow()).thenReturn(belowLow);
        when(belowTrue.getHigh()).thenReturn(belowHigh);

        Range aboveTrue = mock(Range.class);
        Marker aboveLow = mock(Marker.class);
        Marker aboveHigh = mock(Marker.class);
        when(aboveLow.isLowerUnbounded()).thenReturn(false);
        when(aboveLow.getBound()).thenReturn(Marker.Bound.ABOVE);
        when(aboveLow.getValue()).thenReturn(true);
        when(aboveHigh.isUpperUnbounded()).thenReturn(true);
        when(aboveTrue.getLow()).thenReturn(aboveLow);
        when(aboveTrue.getHigh()).thenReturn(aboveHigh);

        when(ranges.getOrderedRanges()).thenReturn(Arrays.asList(belowTrue, aboveTrue));
        when(valueSet.getRanges()).thenReturn(ranges);

        Map<String, ValueSet> constraints = new HashMap<>();
        constraints.put("field_checkbox", valueSet);

        List<AthenaFieldLarkBaseMapping> mappings = Collections.singletonList(
            new AthenaFieldLarkBaseMapping("field_checkbox", "Checkbox Field",
                new NestedUIType(UITypeEnum.CHECKBOX, null)));

        String filterJson = SearchApiFilterTranslator.toFilterJson(constraints, mappings);

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        assertNull(filter.get("children"));
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("false", conditions.get(0).get("value").get(0).asText());
    }

    // ========== Tests for addEmptinessCondition (ORDER BY ... NULLS FIRST two-phase fetch) ==========

    @Test
    public void testAddEmptinessCondition_blankFilter_createsNewAndFilterWithIsEmpty() throws Exception {
        String result = SearchApiFilterTranslator.addEmptinessCondition("", "Currency Field", true);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        assertEquals("and", filter.get("conjunction").asText());
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Currency Field", conditions.get(0).get("field_name").asText());
        assertEquals("isEmpty", conditions.get(0).get("operator").asText());
    }

    @Test
    public void testAddEmptinessCondition_nullFilter_createsNewAndFilter() throws Exception {
        String result = SearchApiFilterTranslator.addEmptinessCondition(null, "Currency Field", false);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        assertEquals("isNotEmpty", filter.get("conditions").get(0).get("operator").asText());
    }

    @Test
    public void testAddEmptinessCondition_wantEmptyFalse_usesIsNotEmptyOperator() throws Exception {
        String result = SearchApiFilterTranslator.addEmptinessCondition("", "Currency Field", false);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        assertEquals("isNotEmpty", filter.get("conditions").get(0).get("operator").asText());
    }

    @Test
    public void testAddEmptinessCondition_existingFilter_preservesConditionsAndAppendsNewOne() throws Exception {
        // The nulls-first two-phase fetch must AND its isEmpty/isNotEmpty condition onto whatever WHERE
        // clause was already pushed down (e.g. "field_status is 'active'"), not replace it.
        String existingFilter = "{\"conjunction\":\"and\",\"conditions\":"
                + "[{\"field_name\":\"Status\",\"operator\":\"is\",\"value\":[\"active\"]}]}";

        String result = SearchApiFilterTranslator.addEmptinessCondition(existingFilter, "Currency Field", true);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size());
        assertEquals("Status", conditions.get(0).get("field_name").asText());
        assertEquals("Currency Field", conditions.get(1).get("field_name").asText());
        assertEquals("isEmpty", conditions.get(1).get("operator").asText());
    }

    @Test
    public void testAddEmptinessCondition_existingFilterWithOrGroups_preservesChildren() throws Exception {
        String existingFilter = "{\"conjunction\":\"and\",\"conditions\":[],"
                + "\"children\":[{\"conjunction\":\"or\",\"conditions\":"
                + "[{\"field_name\":\"Category\",\"operator\":\"is\",\"value\":[\"a\"]}]}]}";

        String result = SearchApiFilterTranslator.addEmptinessCondition(existingFilter, "Currency Field", false);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        assertEquals(1, filter.get("conditions").size());
        assertEquals(1, filter.get("children").size());
        assertEquals("Category", filter.get("children").get(0).get("conditions").get(0).get("field_name").asText());
    }

    @Test
    public void testAddEmptinessCondition_malformedExistingFilter_discardsItAndStillAddsCondition() throws Exception {
        String result = SearchApiFilterTranslator.addEmptinessCondition("{not valid json", "Currency Field", true);

        JsonNode filter = OBJECT_MAPPER.readTree(result);
        assertEquals(1, filter.get("conditions").size());
        assertEquals("Currency Field", filter.get("conditions").get(0).get("field_name").asText());
    }
}
