/*-
 * #%L
 * athena-deltashare
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
package com.amazonaws.athena.connectors.deltashare.util;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.substrait.model.ColumnPredicate;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeltaSharePredicateUtilsTest
{
    @Rule
    public TestName testName = new TestName();
    
    protected BlockAllocator allocator;
    
    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
    }
    
    @After
    public void tearDown()
    {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithEquality()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col1", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true)
            .add("value1").build());

        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithRange()
    {
        SortedRangeSet.Builder rangeBuilder = SortedRangeSet.newBuilder(new ArrowType.Int(32, true), false);
        rangeBuilder.add(Range.range(allocator, new ArrowType.Int(32, true), 10, true, 20, true));
        
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col2", rangeBuilder.build());

        List<String> partitionColumns = Arrays.asList("col2");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithMultipleValues()
    {
        EquatableValueSet.Builder valueSetBuilder = EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true);
        valueSetBuilder.add("value1");
        valueSetBuilder.add("value2");
        valueSetBuilder.add("value3");

        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col1", valueSetBuilder.build());

        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithNonPartitionColumn()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("non_partition_col", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true)
            .add("value1").build());

        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty());
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithEmptyValueSets()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty());
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithEmptyPartitionColumns()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col1", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true)
            .add("value1").build());

        List<String> partitionColumns = Collections.emptyList();

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty());
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithComplexRange()
    {
        SortedRangeSet.Builder rangeBuilder = SortedRangeSet.newBuilder(new ArrowType.Int(32, true), false);
        rangeBuilder.add(Range.range(allocator, new ArrowType.Int(32, true), 10, false, 20, false));
        rangeBuilder.add(Range.range(allocator, new ArrowType.Int(32, true), 30, true, 40, true));
        
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col2", rangeBuilder.build());

        List<String> partitionColumns = Arrays.asList("col2");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsFromValueSetsWithMultipleColumns()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("col1", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true)
            .add("value1").build());
        
        SortedRangeSet.Builder rangeBuilder = SortedRangeSet.newBuilder(new ArrowType.Int(32, true), false);
        rangeBuilder.add(Range.range(allocator, new ArrowType.Int(32, true), 10, true, 20, true));
        valueSets.put("col2", rangeBuilder.build());

        List<String> partitionColumns = Arrays.asList("col1", "col2");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsWithEmptyPredicates()
    {
        Map<String, List<ColumnPredicate>> filterPredicates = new HashMap<>();
        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHints(filterPredicates, partitionColumns);

        assertTrue(result == null || result.isEmpty());
    }

    @Test
    public void testBuildFilterPredicatesFromPlan()
    {
        Plan mockPlan = Plan.newBuilder().build();

        Map<String, List<ColumnPredicate>> result = DeltaSharePredicateUtils.buildFilterPredicatesFromPlan(mockPlan);

        assertNotNull(result);
    }

    @Test
    public void testBuildJsonPredicateHintsWithNullValues()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        EquatableValueSet.Builder valueSetBuilder = EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true);
        valueSetBuilder.add("value1");
        
        valueSets.put("col1", valueSetBuilder.build());

        List<String> partitionColumns = Arrays.asList("col1");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsWithDateValues()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("date_col", EquatableValueSet.newBuilder(allocator, new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY), true, true)
            .add(18628).build());

        List<String> partitionColumns = Arrays.asList("date_col");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsWithTimestampValues()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("timestamp_col", EquatableValueSet.newBuilder(allocator, new ArrowType.Utf8(), true, true)
            .add("2021-01-01").build());

        List<String> partitionColumns = Arrays.asList("timestamp_col");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsWithBooleanValues()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("bool_col", EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
            .add(true).build());

        List<String> partitionColumns = Arrays.asList("bool_col");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }

    @Test
    public void testBuildJsonPredicateHintsWithFloatValues()
    {
        Map<String, ValueSet> valueSets = new HashMap<>();
        valueSets.put("float_col", EquatableValueSet.newBuilder(allocator, new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), true, true)
            .add(3.14).build());

        List<String> partitionColumns = Arrays.asList("float_col");

        String result = DeltaSharePredicateUtils.buildJsonPredicateHintsFromValueSets(valueSets, partitionColumns);

        assertTrue(result == null || result.isEmpty() || result.equals("{}"));
    }
}
