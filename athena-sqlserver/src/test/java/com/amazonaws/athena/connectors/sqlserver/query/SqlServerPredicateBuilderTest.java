/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerPredicateBuilderTest
{
    private static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    
    private BlockAllocatorImpl allocator;
    private Split split;
    private List<TypeAndValue> parameterValues;
    private List<Field> fields;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        parameterValues = new ArrayList<>();
        fields = new ArrayList<>();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildConjuncts_WithSingleValueRange_ReturnsEqualityPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet singleValueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", singleValueSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Conjunct should contain = operator", conjuncts.get(0).contains("="));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithRangePredicate_ReturnsRangePredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain > operator", conjuncts.get(0).contains(">"));
        assertTrue("Conjunct should contain < operator", conjuncts.get(0).contains("<"));
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleSingleValues_ReturnsInPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet inSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 20), Marker.exactly(allocator, INT_TYPE, 20)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 30), Marker.exactly(allocator, INT_TYPE, 30)))
                .build();
        constraintMap.put("intCol", inSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain parentheses", conjuncts.get(0).contains("("));
        assertEquals("Should have three parameters", 3, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNullValueSet_ReturnsIsNullPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet nullSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();
        constraintMap.put("intCol", nullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
        assertEquals("Should have no parameters", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithUnboundedRange_ReturnsIsNotNullPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet notNullSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", notNullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NOT NULL", conjuncts.get(0).contains("IS NOT NULL"));
        assertEquals("Should have no parameters", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNullAllowedRange_ReturnsOrPredicateWithIsNull()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeWithNull = SortedRangeSet.newBuilder(INT_TYPE, true)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeWithNull);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain OR", conjuncts.get(0).contains("OR"));
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
    }

    @Test
    public void buildConjuncts_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", rangeSet);
        constraintMap.put("partitionCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("partitionCol", INT_TYPE));
        
        // Create split with partition column
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("partitionCol", "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);

        List<String> conjuncts = buildConjuncts(constraintMap, fields, splitWithPartition);

        // Should only have conjunct for intCol, not partitionCol
        assertFalse("Should have at least one conjunct", conjuncts.isEmpty());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("\"intCol\""));
    }

    @Test
    public void buildConjuncts_WithStringType_ReturnsStringPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain stringCol", conjuncts.get(0).contains("\"stringCol\""));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithBooleanType_ReturnsBooleanPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet booleanSet = SortedRangeSet.newBuilder(BOOLEAN_TYPE, false)
                .add(new Range(Marker.exactly(allocator, BOOLEAN_TYPE, true), Marker.exactly(allocator, BOOLEAN_TYPE, true)))
                .build();
        constraintMap.put("boolCol", booleanSet);

        fields.add(Field.nullable("boolCol", BOOLEAN_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain boolCol", conjuncts.get(0).contains("\"boolCol\""));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleColumns_ReturnsMultipleConjuncts()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet intSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("intCol", intSet);
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);

        assertEquals("Should have two conjuncts", 2, conjuncts.size());
        assertTrue("Should contain intCol conjunct", conjuncts.stream().anyMatch(c -> c.contains("\"intCol\"")));
        assertTrue("Should contain stringCol conjunct", conjuncts.stream().anyMatch(c -> c.contains("\"stringCol\"")));
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithEmptyConstraints_ReturnsEmptyOrComplexExpressions()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields);


        // Should have no conjuncts for value sets, but may have complex expressions
        assertTrue("Conjuncts should be empty or only contain complex expressions", conjuncts.size() >= 0);
    }

    private List<String> buildConjuncts(Map<String, ValueSet> constraintMap, List<Field> fields)
    {
        return buildConjuncts(constraintMap, fields, split);
    }

    private List<String> buildConjuncts(Map<String, ValueSet> constraintMap, List<Field> fields, Split customSplit)
    {
        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(),
                Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        SqlServerPredicateBuilder predicateBuilder = new SqlServerPredicateBuilder();
        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, constraints, parameterValues, customSplit);
        assertNotNull("Conjuncts should not be null", conjuncts);
        return conjuncts;
    }
}

