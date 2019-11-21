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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.FLOAT8;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;
import static org.junit.Assert.*;

public class RangeTest
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedTypes()
            throws Exception
    {
        // NEVER DO THIS
        new Range(Marker.exactly(allocator, BIGINT.getType(), 1L), Marker.exactly(allocator, VARCHAR.getType(), "a"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvertedBounds()
            throws Exception
    {
        new Range(Marker.exactly(allocator, BIGINT.getType(), 1L), Marker.exactly(allocator, BIGINT.getType(), 0L));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLowerUnboundedOnly()
            throws Exception
    {
        new Range(Marker.lowerUnbounded(allocator, BIGINT.getType()), Marker.lowerUnbounded(allocator, BIGINT.getType()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpperUnboundedOnly()
            throws Exception
    {
        new Range(Marker.upperUnbounded(allocator, BIGINT.getType()), Marker.upperUnbounded(allocator, BIGINT.getType()));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        assertTrue(Range.range(allocator, BIGINT.getType(), 1L, true, 1L, true).isSingleValue());
        assertFalse(Range.range(allocator, BIGINT.getType(), 1L, true, 2L, true).isSingleValue());
        assertTrue(Range.range(allocator, FLOAT8.getType(), 1.1, true, 1.1, true).isSingleValue());
        assertTrue(Range.range(allocator, VARCHAR.getType(), "a", true, "a", true).isSingleValue());
        assertTrue(Range.range(allocator, BIT.getType(), true, true, true, true).isSingleValue());
        assertFalse(Range.range(allocator, BIT.getType(), false, true, true, true).isSingleValue());
    }

    @Test
    public void testAllRange()
            throws Exception
    {
        Range range = Range.all(allocator, BIGINT.getType());
        assertEquals(range.getLow(), Marker.lowerUnbounded(allocator, BIGINT.getType()));
        assertEquals(range.getHigh(), Marker.upperUnbounded(allocator, BIGINT.getType()));
        assertFalse(range.isSingleValue());
        assertTrue(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertTrue(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertTrue(range.includes(Marker.below(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.above(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testGreaterThanRange()
            throws Exception
    {
        Range range = Range.greaterThan(allocator, BIGINT.getType(), 1L);
        assertEquals(range.getLow(), Marker.above(allocator, BIGINT.getType(), 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(allocator, BIGINT.getType()));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertFalse(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 2L)));
        assertTrue(range.includes(new LiteralValueMarker(2L, BIGINT.getType())));
        assertTrue(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testGreaterThanOrEqualRange()
            throws Exception
    {
        Range range = Range.greaterThanOrEqual(allocator, BIGINT.getType(), 1L);
        assertEquals(range.getLow(), Marker.exactly(allocator, BIGINT.getType(), 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(allocator, BIGINT.getType()));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertFalse(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 2L)));
        assertTrue(range.includes(new LiteralValueMarker(2L, BIGINT.getType())));
        assertTrue(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testLessThanRange()
            throws Exception
    {
        Range range = Range.lessThan(allocator, BIGINT.getType(), 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(allocator, BIGINT.getType()));
        assertEquals(range.getHigh(), Marker.below(allocator, BIGINT.getType(), 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertTrue(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(range.includes(new LiteralValueMarker(0L, BIGINT.getType())));
        assertFalse(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testLessThanOrEqualRange()
            throws Exception
    {
        Range range = Range.lessThanOrEqual(allocator, BIGINT.getType(), 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(allocator, BIGINT.getType()));
        assertEquals(range.getHigh(), Marker.exactly(allocator, BIGINT.getType(), 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertTrue(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 2L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertFalse(range.includes(new LiteralValueMarker(2L, BIGINT.getType())));
        assertFalse(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testEqualRange()
            throws Exception
    {
        Range range = Range.equal(allocator, BIGINT.getType(), 1L);
        assertEquals(range.getLow(), Marker.exactly(allocator, BIGINT.getType(), 1L));
        assertEquals(range.getHigh(), Marker.exactly(allocator, BIGINT.getType(), 1L));
        assertTrue(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertFalse(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 2L)));
        assertFalse(range.includes(new LiteralValueMarker(2L, BIGINT.getType())));
        assertFalse(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testRange()
            throws Exception
    {
        Range range = Range.range(allocator, BIGINT.getType(), 0L, false, 2L, true);
        assertEquals(range.getLow(), Marker.above(allocator, BIGINT.getType(), 0L));
        assertEquals(range.getHigh(), Marker.exactly(allocator, BIGINT.getType(), 2L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT.getType());
        assertFalse(range.includes(Marker.lowerUnbounded(allocator, BIGINT.getType())));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 0L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 1L)));
        assertTrue(range.includes(Marker.exactly(allocator, BIGINT.getType(), 2L)));
        assertFalse(range.includes(Marker.exactly(allocator, BIGINT.getType(), 3L)));
        assertFalse(range.includes(new LiteralValueMarker(0L, BIGINT.getType())));
        assertTrue(range.includes(new LiteralValueMarker(1L, BIGINT.getType())));
        assertTrue(range.includes(new LiteralValueMarker(2L, BIGINT.getType())));
        assertFalse(range.includes(new LiteralValueMarker(3L, BIGINT.getType())));
        assertFalse(range.includes(Marker.upperUnbounded(allocator, BIGINT.getType())));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        assertEquals(Range.equal(allocator, BIGINT.getType(), 0L).getSingleValue(), 0L);
        try {
            Range.lessThan(allocator, BIGINT.getType(), 0L).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testContains()
            throws Exception
    {
        assertTrue(Range.all(allocator, BIGINT.getType()).contains(Range.all(allocator, BIGINT.getType())));
        assertTrue(Range.all(allocator, BIGINT.getType()).contains(Range.equal(allocator, BIGINT.getType(), 0L)));
        assertTrue(Range.all(allocator, BIGINT.getType()).contains(Range.greaterThan(allocator, BIGINT.getType(), 0L)));
        assertTrue(Range.equal(allocator, BIGINT.getType(), 0L).contains(Range.equal(allocator, BIGINT.getType(), 0L)));
        assertFalse(Range.equal(allocator, BIGINT.getType(), 0L).contains(Range.greaterThan(allocator, BIGINT.getType(), 0L)));
        assertFalse(Range.equal(allocator, BIGINT.getType(), 0L).contains(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L)));
        assertFalse(Range.equal(allocator, BIGINT.getType(), 0L).contains(Range.all(allocator, BIGINT.getType())));
        assertTrue(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L).contains(Range.greaterThan(allocator, BIGINT.getType(), 0L)));
        assertTrue(Range.greaterThan(allocator, BIGINT.getType(), 0L).contains(Range.greaterThan(allocator, BIGINT.getType(), 1L)));
        assertFalse(Range.greaterThan(allocator, BIGINT.getType(), 0L).contains(Range.lessThan(allocator, BIGINT.getType(), 0L)));
        assertTrue(Range.range(allocator, BIGINT.getType(), 0L, true, 2L, true).contains(Range.range(allocator, BIGINT.getType(), 1L, true, 2L, true)));
        assertFalse(Range.range(allocator, BIGINT.getType(), 0L, true, 2L, true).contains(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false)));
    }

    @Test
    public void testSpan()
            throws Exception
    {
        assertEquals(Range.greaterThan(allocator, BIGINT.getType(), 1L).span(Range.lessThanOrEqual(allocator, BIGINT.getType(), 2L)), Range.all(allocator, BIGINT.getType()));
        assertEquals(Range.greaterThan(allocator, BIGINT.getType(), 2L).span(Range.lessThanOrEqual(allocator, BIGINT.getType(), 0L)), Range.all(allocator, BIGINT.getType()));
        assertEquals(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).span(Range.equal(allocator, BIGINT.getType(), 2L)), Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false));
        assertEquals(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).span(Range.range(allocator, BIGINT.getType(), 2L, false, 10L, false)), Range.range(allocator, BIGINT.getType(), 1L, true, 10L, false));
        assertEquals(Range.greaterThan(allocator, BIGINT.getType(), 1L).span(Range.equal(allocator, BIGINT.getType(), 0L)), Range.greaterThanOrEqual(allocator, BIGINT.getType(), 0L));
        assertEquals(Range.greaterThan(allocator, BIGINT.getType(), 1L).span(Range.greaterThanOrEqual(allocator, BIGINT.getType(), 10L)), Range.greaterThan(allocator, BIGINT.getType(), 1L));
        assertEquals(Range.lessThan(allocator, BIGINT.getType(), 1L).span(Range.lessThanOrEqual(allocator, BIGINT.getType(), 1L)), Range.lessThanOrEqual(allocator, BIGINT.getType(), 1L));
        assertEquals(Range.all(allocator, BIGINT.getType()).span(Range.lessThanOrEqual(allocator, BIGINT.getType(), 1L)), Range.all(allocator, BIGINT.getType()));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        assertTrue(Range.greaterThan(allocator, BIGINT.getType(), 1L).overlaps(Range.lessThanOrEqual(allocator, BIGINT.getType(), 2L)));
        assertFalse(Range.greaterThan(allocator, BIGINT.getType(), 2L).overlaps(Range.lessThan(allocator, BIGINT.getType(), 2L)));
        assertTrue(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).overlaps(Range.equal(allocator, BIGINT.getType(), 2L)));
        assertTrue(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).overlaps(Range.range(allocator, BIGINT.getType(), 2L, false, 10L, false)));
        assertFalse(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).overlaps(Range.range(allocator, BIGINT.getType(), 3L, true, 10L, false)));
        assertTrue(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, true).overlaps(Range.range(allocator, BIGINT.getType(), 3L, true, 10L, false)));
        assertTrue(Range.all(allocator, BIGINT.getType()).overlaps(Range.equal(allocator, BIGINT.getType(), Long.MAX_VALUE)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        assertEquals(Range.greaterThan(allocator, BIGINT.getType(), 1L).intersect(Range.lessThanOrEqual(allocator, BIGINT.getType(), 2L)), Range.range(allocator, BIGINT.getType(), 1L, false, 2L, true));
        assertEquals(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).intersect(Range.equal(allocator, BIGINT.getType(), 2L)), Range.equal(allocator, BIGINT.getType(), 2L));
        assertEquals(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).intersect(Range.range(allocator, BIGINT.getType(), 2L, false, 10L, false)), Range.range(allocator, BIGINT.getType(), 2L, false, 3L, false));
        assertEquals(Range.range(allocator, BIGINT.getType(), 1L, true, 3L, true).intersect(Range.range(allocator, BIGINT.getType(), 3L, true, 10L, false)), Range.equal(allocator, BIGINT.getType(), 3L));
        assertEquals(Range.all(allocator, BIGINT.getType()).intersect(Range.equal(allocator, BIGINT.getType(), Long.MAX_VALUE)), Range.equal(allocator, BIGINT.getType(), Long.MAX_VALUE));
    }

    @Test
    public void testExceptionalIntersect()
            throws Exception
    {
        try {
            Range.greaterThan(allocator, BIGINT.getType(), 2L).intersect(Range.lessThan(allocator, BIGINT.getType(), 2L));
            fail();
        }
        catch (IllegalArgumentException e) {
        }

        try {
            Range.range(allocator, BIGINT.getType(), 1L, true, 3L, false).intersect(Range.range(allocator, BIGINT.getType(), 3L, true, 10L, false));
            fail();
        }
        catch (IllegalArgumentException e) {
        }
    }
}
