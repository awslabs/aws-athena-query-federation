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

        assertNotNull(filterJson);
        JsonNode filter = OBJECT_MAPPER.readTree(filterJson);
        JsonNode conditions = filter.get("conditions");
        assertEquals(1, conditions.size());
        assertEquals("Checkbox Field", conditions.get(0).get("field_name").asText());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("true", conditions.get(0).get("value").get(0).asText());
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
    public void testToFilterJson_withSortedRangeSet_ranges_exception() {
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
        JsonNode conditions = filter.get("conditions");
        assertEquals(2, conditions.size());
        assertEquals("Text Field", conditions.get(0).get("field_name").asText());
        assertEquals("is", conditions.get(0).get("operator").asText());
        assertEquals("value1", conditions.get(0).get("value").get(0).asText());
        assertEquals("is", conditions.get(1).get("operator").asText());
        assertEquals("value2", conditions.get(1).get("value").get(0).asText());
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
        assertEquals(1, conditions.size());
        assertEquals("isNot", conditions.get(0).get("operator").asText());
        assertEquals("excluded", conditions.get(0).get("value").get(0).asText());
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
    public void testToFilterJson_withAllOrNoneValueSet_isNull_checkbox_skipped() {
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
    public void testToFilterJson_noMappingFound() {
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
    public void testToFilterJson_unsupportedUIType() {
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
    public void testToFilterJson_supportedUITypes() {
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
    public void testToSortJson_nullColumnName() {
        List<OrderByField> orderByFields = Collections.singletonList(
            new OrderByField(null, OrderByField.Direction.ASC_NULLS_FIRST));

        List<AthenaFieldLarkBaseMapping> mappings = Collections.emptyList();

        String sortJson = SearchApiFilterTranslator.toSortJson(orderByFields, mappings);

        // Should return empty string for null column name
        assertEquals("", sortJson);
    }

    @Test
    public void testToSortJson_emptyColumnName() {
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
    public void testToSplitFilterJson_invalidJson() {
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

    // ========== Additional edge case tests ==========

    @Test
    public void testToFilterJson_nullMappingsParameter() {
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
        // Null value should be converted to empty string for non-checkbox
        assertEquals("", conditions.get(0).get("value").get(0).asText());
    }

    @Test
    public void testIsEffectivelyNotNull_exceptionHandling() {
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
    public void testFindMappingForColumn_withNullMappings() {
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
}
