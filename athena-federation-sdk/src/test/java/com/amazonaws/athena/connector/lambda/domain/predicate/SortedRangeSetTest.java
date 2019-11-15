package com.amazonaws.athena.connector.lambda.domain.predicate;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.junit.Assert.*;

public class SortedRangeSetTest
{
    private BlockAllocatorImpl allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testEmptySet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(BIGINT.getType());
        assertEquals(rangeSet.getType(), BIGINT.getType());
        assertTrue(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.isEmpty(rangeSet.getOrderedRanges()));
        assertEquals(rangeSet.getRangeCount(), 0);
        assertEquals(rangeSet.complement(allocator), SortedRangeSet.all(allocator, BIGINT.getType()));
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testEntireSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(allocator, BIGINT.getType());
        assertEquals(rangeSet.getType(), BIGINT.getType());
        assertFalse(rangeSet.isNone());
        assertTrue(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(allocator), SortedRangeSet.none(BIGINT.getType()));
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testNullability()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(allocator, BIGINT.getType(), true, 10L, Collections.singletonList(10L));
        assertTrue(rangeSet.containsValue(Marker.nullMarker(allocator, BIGINT.getType())));
        assertFalse(rangeSet.containsValue(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(rangeSet.containsValue(Marker.exactly(allocator, BIGINT.getType(), 10L)));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(allocator, BIGINT.getType(), 10L);

        SortedRangeSet complement = SortedRangeSet.of(true,
                Range.greaterThan(allocator, BIGINT.getType(), 10L),
                Range.lessThan(allocator, BIGINT.getType(), 10L));

        assertEquals(rangeSet.getType(), BIGINT.getType());
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertTrue(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), ImmutableList.of(Range.equal(allocator, BIGINT.getType(), 10L))));
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(allocator), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 10L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testBoundedSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(allocator, BIGINT.getType(), 10L),
                Range.equal(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 9L, true, 11L, false),
                Range.equal(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 2L, true, 4L, true),
                Range.range(allocator, BIGINT.getType(), 4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 2L, true, 5L, true),
                Range.range(allocator, BIGINT.getType(), 9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(true,
                Range.lessThan(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 0L, false, 2L, false),
                Range.range(allocator, BIGINT.getType(), 5L, false, 9L, false),
                Range.greaterThanOrEqual(allocator, BIGINT.getType(), 11L));

        assertEquals(rangeSet.getType(), BIGINT.getType());
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT.getType(), normalizedResult, false));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(allocator), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 7L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testUnboundedSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(allocator, BIGINT.getType(), 10L),
                Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 2L, true, 4L, false),
                Range.range(allocator, BIGINT.getType(), 4L, true, 6L, false),
                Range.range(allocator, BIGINT.getType(), 1L, false, 2L, false),
                Range.range(allocator, BIGINT.getType(), 9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L),
                Range.range(allocator, BIGINT.getType(), 1L, false, 6L, false),
                Range.greaterThan(allocator, BIGINT.getType(), 9L));

        SortedRangeSet complement = SortedRangeSet.of(true,
                Range.range(allocator, BIGINT.getType(), 0L, false, 1L, true),
                Range.range(allocator, BIGINT.getType(), 6L, true, 9L, true));

        assertEquals(rangeSet.getType(), BIGINT.getType());
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT.getType(), normalizedResult, false));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(allocator), complement);
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 4L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(allocator, BIGINT.getType(), 7L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        assertEquals(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).getSingleValue(), 0L);
        try {
            SortedRangeSet.all(allocator, BIGINT.getType()).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testSpan()
            throws Exception
    {
        try {
            SortedRangeSet.none(BIGINT.getType()).getSpan();
            fail();
        }
        catch (IllegalStateException e) {
        }

        assertEquals(SortedRangeSet.all(allocator, BIGINT.getType()).getSpan(), Range.all(allocator, BIGINT.getType()));
        assertEquals(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).getSpan(), Range.equal(allocator, BIGINT.getType(), 0L));
        assertEquals(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).getSpan(), Range.range(allocator, BIGINT.getType(), 0L, true, 1L, true));
        assertEquals(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.greaterThan(allocator, BIGINT.getType(), 1L)).getSpan(), Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L));
        assertEquals(SortedRangeSet.of(Range.lessThan(allocator, BIGINT.getType(), 0L), Range.greaterThan(allocator, BIGINT.getType(), 1L)).getSpan(), Range.all(allocator, BIGINT.getType()));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertFalse(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertTrue(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertTrue(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertTrue(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).overlaps(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).overlaps(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertTrue(SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).overlaps(allocator, SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(allocator, BIGINT.getType(), 0L)).overlaps(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
    }

    @Test
    public void testContains()
            throws Exception
    {
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertTrue(SortedRangeSet.all(allocator, BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertFalse(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertTrue(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.none(BIGINT.getType()).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.all(allocator, BIGINT.getType())));
        assertTrue(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.none(BIGINT.getType())));
        assertTrue(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.of(allocator, BIGINT.getType(), 0L).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L), Range.lessThan(allocator, BIGINT.getType(), 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).contains(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).contains(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L), Range.equal(allocator, BIGINT.getType(), 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).contains(allocator, SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(allocator, BIGINT.getType(), 0L)).contains(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).intersect(allocator,
                        SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));

        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).intersect(allocator,
                        SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.all(allocator, BIGINT.getType()));

        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).intersect(allocator,
                        SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));

        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L), Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 3L)).intersect(allocator,
                        SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 4L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L)));

        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).intersect(allocator,
                        SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 4L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 4L)));

        assertEquals(
                SortedRangeSet.of(Range.range(allocator, BIGINT.getType(), 0L, true, 4L, false)).intersect(allocator,
                        SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.greaterThan(allocator, BIGINT.getType(), 3L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.range(allocator, BIGINT.getType(), 3L, false, 4L, false)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)).intersect(allocator,
                        SortedRangeSet.of(Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), -1L)).intersect(allocator,
                        SortedRangeSet.of(Range.lessThanOrEqual(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.of(Range.range(allocator, BIGINT.getType(), -1L, true, 1L, true)));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertUnion(SortedRangeSet.none(BIGINT.getType()), SortedRangeSet.none(BIGINT.getType()), SortedRangeSet.none(BIGINT.getType()));
        assertUnion(SortedRangeSet.all(allocator, BIGINT.getType()), SortedRangeSet.all(allocator, BIGINT.getType()), SortedRangeSet.all(allocator, BIGINT.getType()));
        assertUnion(SortedRangeSet.none(BIGINT.getType()), SortedRangeSet.all(allocator, BIGINT.getType()), SortedRangeSet.all(allocator, BIGINT.getType()));

        assertUnion(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L), Range.equal(allocator, BIGINT.getType(), 2L)),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 3L)),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 1L), Range.equal(allocator, BIGINT.getType(), 2L), Range.equal(allocator, BIGINT.getType(), 3L)));

        assertUnion(SortedRangeSet.all(allocator, BIGINT.getType()), SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L)), SortedRangeSet.all(allocator, BIGINT.getType()));

        assertUnion(
                SortedRangeSet.of(Range.range(allocator, BIGINT.getType(), 0L, true, 4L, false)),
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 3L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(Range.all(allocator, BIGINT.getType())));

        assertUnion(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(true, Range.lessThan(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).complement(allocator));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).subtract(allocator, SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).subtract(allocator, SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.all(allocator, BIGINT.getType()));
        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).subtract(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).complement(allocator));
        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).subtract(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).complement(allocator));
        assertEquals(
                SortedRangeSet.all(allocator, BIGINT.getType()).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.of(true, Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L)));

        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).subtract(allocator, SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).subtract(allocator, SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).subtract(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).subtract(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.none(BIGINT.getType()).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.none(BIGINT.getType()));

        assertEquals(
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L));
        assertEquals(
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.none(BIGINT.getType()));

        SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)));

        assertEquals(
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.of(allocator, BIGINT.getType(), 0L));

        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).subtract(allocator, SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).subtract(allocator, SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)));
        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).subtract(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(allocator, BIGINT.getType(), 1L));
        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).subtract(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L)).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).subtract(allocator, SortedRangeSet.all(allocator, BIGINT.getType())),
                SortedRangeSet.none(BIGINT.getType()));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).subtract(allocator, SortedRangeSet.none(BIGINT.getType())),
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).subtract(allocator, SortedRangeSet.of(allocator, BIGINT.getType(), 0L)),
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).subtract(allocator, SortedRangeSet.of(Range.equal(allocator, BIGINT.getType(), 0L), Range.equal(allocator, BIGINT.getType(), 1L))),
                SortedRangeSet.of(Range.range(allocator, BIGINT.getType(), 0L, false, 1L, false), Range.greaterThan(allocator, BIGINT.getType(), 1L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L)).subtract(allocator, SortedRangeSet.of(Range.greaterThan(allocator, BIGINT.getType(), 0L))),
                SortedRangeSet.none(BIGINT.getType()));
    }

    private void assertUnion(SortedRangeSet first, SortedRangeSet second, SortedRangeSet expected)
    {
        assertEquals(first.union(allocator, second), expected);
        assertEquals(first.union(allocator, ImmutableList.of(first, second)), expected);
    }
}
