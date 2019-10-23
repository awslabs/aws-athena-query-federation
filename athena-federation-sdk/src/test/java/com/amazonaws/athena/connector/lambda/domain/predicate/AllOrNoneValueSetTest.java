package com.amazonaws.athena.connector.lambda.domain.predicate;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AllOrNoneValueSetTest
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
    public void testAll()
            throws Exception
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        assertEquals(valueSet.getType(), Types.MinorType.INT.getType());
        assertFalse(valueSet.isNone());
        assertTrue(valueSet.isAll());
        assertFalse(valueSet.isSingleValue());
        assertTrue(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));

        try {
            valueSet.getSingleValue();
            fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testNone()
            throws Exception
    {
        AllOrNoneValueSet valueSet = AllOrNoneValueSet.none(Types.MinorType.INT.getType());
        assertEquals(valueSet.getType(), Types.MinorType.INT.getType());
        assertTrue(valueSet.isNone());
        assertFalse(valueSet.isAll());
        assertFalse(valueSet.isSingleValue());
        assertFalse(valueSet.containsValue(Marker.exactly(allocator, Types.MinorType.INT.getType(), 0)));

        try {
            valueSet.getSingleValue();
            fail();
        }
        catch (Exception ignored) {
        }
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertEquals(all.intersect(allocator, all), all);
        assertEquals(all.intersect(allocator, none), none);
        assertEquals(none.intersect(allocator, all), none);
        assertEquals(none.intersect(allocator, none), none);
    }

    @Test
    public void testUnion()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertEquals(all.union(allocator, all), all);
        assertEquals(all.union(allocator, none), all);
        assertEquals(none.union(allocator, all), all);
        assertEquals(none.union(allocator, none), none);
    }

    @Test
    public void testComplement()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertEquals(all.complement(allocator), none);
        assertEquals(none.complement(allocator), all);
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertTrue(all.overlaps(allocator, all));
        assertFalse(all.overlaps(allocator, none));
        assertFalse(none.overlaps(allocator, all));
        assertFalse(none.overlaps(allocator, none));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertEquals(all.subtract(allocator, all), none);
        assertEquals(all.subtract(allocator, none), all);
        assertEquals(none.subtract(allocator, all), none);
        assertEquals(none.subtract(allocator, none), none);
    }

    @Test
    public void testContains()
            throws Exception
    {
        AllOrNoneValueSet all = AllOrNoneValueSet.all(Types.MinorType.INT.getType());
        AllOrNoneValueSet none = AllOrNoneValueSet.none(Types.MinorType.INT.getType());

        assertTrue(all.contains(allocator, all));
        assertTrue(all.contains(allocator, none));
        assertFalse(none.contains(allocator, all));
        assertTrue(none.contains(allocator, none));
    }
}
