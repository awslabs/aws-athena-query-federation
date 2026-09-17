/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connectors.timestream.TestUtils;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PredicateBuilderTest
{
    private BlockAllocator allocator;
    private static final ArrowType INT = Types.MinorType.INT.getType();
    private static final ArrowType BIGINT = Types.MinorType.BIGINT.getType();
    private static final ArrowType VARCHAR = Types.MinorType.VARCHAR.getType();
    private static final ArrowType TS = Types.MinorType.DATEMILLI.getType();

    @Before
    public void setup()
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
    public void buildConjuncts_emptySummary_returnsEmptyList()
    {
        List<String> conjuncts = PredicateBuilder.buildConjucts(
                TestUtils.constraints(Collections.emptyMap(), Collections.emptyMap()));
        assertTrue(conjuncts.isEmpty());
    }

    @Test
    public void buildConjuncts_onlyNull_returnsIsNull()
    {
        Map<String, ValueSet> summary = ImmutableMap.of("c", SortedRangeSet.onlyNull(INT));
        List<String> out = PredicateBuilder.buildConjucts(TestUtils.constraints(summary, Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"c\" IS NULL)"), out);
    }

    @Test
    public void buildConjuncts_notNull_returnsIsNotNull()
    {
        Map<String, ValueSet> summary = ImmutableMap.of("c", SortedRangeSet.notNull(allocator, INT));
        List<String> out = PredicateBuilder.buildConjucts(TestUtils.constraints(summary, Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"c\" IS NOT NULL)"), out);
    }

    @Test
    public void buildConjuncts_greaterThanInt_usesStrictLowBound()
    {
        ValueSet vs = SortedRangeSet.copyOf(INT, ImmutableList.of(Range.greaterThan(allocator, INT, 5)), false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" > 5))"), out);
    }

    @Test
    public void buildConjuncts_lessThanInt_usesStrictHighBound()
    {
        ValueSet vs = SortedRangeSet.copyOf(INT, ImmutableList.of(Range.lessThan(allocator, INT, 9)), false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" < 9))"), out);
    }

    @Test
    public void buildConjuncts_boundedInclusive_usesGteAndLte()
    {
        ValueSet vs = SortedRangeSet.copyOf(
                INT,
                ImmutableList.of(Range.range(allocator, INT, 1, true, 10, true)),
                false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" >= 1 AND \"n\" <= 10))"), out);
    }

    @Test
    public void buildConjuncts_boundedExclusive_usesGtAndLt()
    {
        ValueSet vs = SortedRangeSet.copyOf(
                INT,
                ImmutableList.of(Range.range(allocator, INT, 1, false, 10, false)),
                false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" > 1 AND \"n\" < 10))"), out);
    }

    @Test
    public void buildConjuncts_singleEquality_usesEquals()
    {
        ValueSet vs = SortedRangeSet.copyOf(INT, ImmutableList.of(Range.equal(allocator, INT, 42)), false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"n\" = 42)"), out);
    }

    @Test
    public void buildConjuncts_multipleDiscreteValues_usesIn()
    {
        ValueSet vs = SortedRangeSet.newBuilder(INT, false)
                .add(Range.equal(allocator, INT, 2))
                .add(Range.equal(allocator, INT, 3))
                .build();
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"n\" IN (2,3))"), out);
    }

    @Test
    public void buildConjuncts_nullAllowedWithGreaterThan_includesNullDisjunct()
    {
        ValueSet vs = SortedRangeSet.copyOf(INT, ImmutableList.of(Range.greaterThan(allocator, INT, 0)), true);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" IS NULL) OR (\"n\" > 0))"), out);
    }

    @Test
    public void buildConjuncts_timestampEquality_usesQuotedLiteral()
    {
        LocalDateTime t = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_456_789);
        ValueSet vs = SortedRangeSet.copyOf(TS, ImmutableList.of(Range.equal(allocator, TS, t)), false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("ts", vs), Collections.emptyMap()));
        // Range/allocator may normalize to millisecond precision in the value marker.
        assertEquals(1, out.size());
        assertTrue(
                "got: " + out.get(0),
                out.get(0).equals("(\"ts\" = '2024-06-15 12:30:45.123456789')")
                        || out.get(0).equals("(\"ts\" = '2024-06-15 12:30:45.123000000')"));
    }

    @Test
    public void buildConjuncts_bigintEquality_unquotedNumeric()
    {
        ValueSet vs = SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(allocator, BIGINT, 9_000_000_000L)), false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("id", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"id\" = 9000000000)"), out);
    }

    @Test
    public void buildConjuncts_equatableValueSet_usesIn()
    {
        ValueSet vs = EquatableValueSet.newBuilder(allocator, VARCHAR, true, false)
                .add("a")
                .add("b")
                .build();
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("s", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"s\" IN ('a','b'))"), out);
    }

    @Test
    public void buildConjuncts_equatableSingleValue_usesInWithOneElement()
    {
        ValueSet vs = EquatableValueSet.newBuilder(allocator, INT, true, false)
                .add(7)
                .build();
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("k", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("(\"k\" IN (7))"), out);
    }

    @Test
    public void buildConjuncts_twoColumns_followsMapIterationOrder()
    {
        Map<String, ValueSet> summary = new LinkedHashMap<>();
        summary.put("z_col", SortedRangeSet.copyOf(INT, ImmutableList.of(Range.equal(allocator, INT, 1)), false));
        summary.put("a_col", SortedRangeSet.copyOf(INT, ImmutableList.of(Range.equal(allocator, INT, 2)), false));
        List<String> out = PredicateBuilder.buildConjucts(TestUtils.constraints(summary, Collections.emptyMap()));
        assertEquals(2, out.size());
        assertEquals("(\"z_col\" = 1)", out.get(0));
        assertEquals("(\"a_col\" = 2)", out.get(1));
    }

    @Test
    public void buildConjuncts_greaterThanOrEqual_usesGteFromExactlyLow()
    {
        ValueSet vs = SortedRangeSet.copyOf(
                INT,
                ImmutableList.of(Range.greaterThanOrEqual(allocator, INT, 100)),
                false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" >= 100))"), out);
    }

    @Test
    public void buildConjuncts_lessThanOrEqual_usesLteFromExactlyHigh()
    {
        ValueSet vs = SortedRangeSet.copyOf(
                INT,
                ImmutableList.of(Range.lessThanOrEqual(allocator, INT, 50)),
                false);
        List<String> out = PredicateBuilder.buildConjucts(
                TestUtils.constraints(ImmutableMap.of("n", vs), Collections.emptyMap()));
        assertEquals(ImmutableList.of("((\"n\" <= 50))"), out);
    }
}
