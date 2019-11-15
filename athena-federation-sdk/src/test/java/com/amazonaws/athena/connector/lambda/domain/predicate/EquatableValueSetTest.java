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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class EquatableValueSetTest
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

    private ArrowType INT = Types.MinorType.INT.getType();

    @Test
    public void testEmptySet()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.none(allocator, INT);
        assertEquals(equatables.getType(), INT);
        assertTrue(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertEquals(equatables.getValues().getRowCount(), 0);
        assertEquals(equatables.complement(allocator), EquatableValueSet.all(allocator, INT));
        assertFalse(equatables.containsValue(0));
        assertFalse(equatables.containsValue(1));
    }

    @Test
    public void testEntireSet()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.all(allocator, INT);
        assertEquals(equatables.getType(), INT);
        assertFalse(equatables.isNone());
        assertTrue(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertFalse(equatables.isWhiteList());
        assertEquals(equatables.getValues().getRowCount(), 0);
        assertEquals(equatables.complement(allocator), EquatableValueSet.none(allocator, INT));
        assertTrue(equatables.containsValue(0));
        assertTrue(equatables.containsValue(1));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.of(allocator, INT, 10);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(allocator, INT).subtract(allocator, equatables);

        // Whitelist
        assertEquals(equatables.getType(), INT);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertTrue(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertEquals(equatables.getSingleValue(), 10);
        assertEquals(equatables.complement(allocator), complement);
        assertFalse(equatables.containsValue(0));
        assertFalse(equatables.containsValue(1));
        assertTrue(equatables.containsValue(10));

        // Blacklist
        assertEquals(complement.getType(), INT);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.isWhiteList());
        assertEquals(complement.toString(), complement.getValue(0), 10);
        assertEquals(complement.complement(allocator), equatables);
        assertTrue(complement.containsValue(0));
        assertTrue(complement.containsValue(1));
        assertFalse(complement.containsValue(10));
    }

    @Test
    public void testMultipleValues()
            throws Exception
    {
        EquatableValueSet equatables = EquatableValueSet.of(allocator, INT, 1, 2, 3, 1);

        EquatableValueSet complement = (EquatableValueSet) EquatableValueSet.all(allocator, INT).subtract(allocator, equatables);

        // Whitelist
        assertEquals(equatables.getType(), INT);
        assertFalse(equatables.isNone());
        assertFalse(equatables.isAll());
        assertFalse(equatables.isSingleValue());
        assertTrue(equatables.isWhiteList());
        assertEquals(equatables.complement(allocator), complement);
        assertFalse(equatables.containsValue(0));
        assertTrue(equatables.containsValue(1));
        assertTrue(equatables.containsValue(2));
        assertTrue(equatables.containsValue(3));
        assertFalse(equatables.containsValue(4));

        // Blacklist
        assertEquals(complement.getType(), INT);
        assertFalse(complement.isNone());
        assertFalse(complement.isAll());
        assertFalse(complement.isSingleValue());
        assertFalse(complement.isWhiteList());
        assertEquals(complement.complement(allocator), equatables);
        assertTrue(complement.containsValue(0));
        assertFalse(complement.containsValue(1));
        assertFalse(complement.containsValue(2));
        assertFalse(complement.containsValue(3));
        assertTrue(complement.containsValue(4));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        assertEquals(EquatableValueSet.of(allocator, INT, 0).getSingleValue(), 0);
        try {
            EquatableValueSet.all(allocator, INT).getSingleValue();
            fail();
        }
        catch (IllegalStateException ignored) {
        }
    }

    @Test
    public void testNullability()
            throws Exception
    {
        ValueSet actual = EquatableValueSet.of(allocator, INT, true, Collections.singletonList(100));
        assertTrue(actual.containsValue(Marker.exactly(allocator, INT, 100)));
        assertTrue(actual.containsValue(Marker.nullMarker(allocator, INT)));
        assertFalse(actual.containsValue(Marker.exactly(allocator, INT, 101)));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        assertTrue(EquatableValueSet.all(allocator, INT).overlaps(allocator, EquatableValueSet.all(allocator, INT)));
        assertFalse(EquatableValueSet.all(allocator, INT).overlaps(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.all(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertTrue(EquatableValueSet.all(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertTrue(EquatableValueSet.all(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));

        assertFalse(EquatableValueSet.none(allocator, INT).overlaps(allocator, EquatableValueSet.all(allocator, INT)));
        assertFalse(EquatableValueSet.none(allocator, INT).overlaps(allocator, EquatableValueSet.none(allocator, INT)));
        assertFalse(EquatableValueSet.none(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertFalse(EquatableValueSet.none(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.none(allocator, INT).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));

        assertTrue(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.all(allocator, INT)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 1)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0).complement(allocator)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0).overlaps(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)));

        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.all(allocator, INT)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.of(allocator, INT, -1)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).overlaps(allocator, EquatableValueSet.of(allocator, INT, -1).complement(allocator)));

        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.all(allocator, INT)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.none(allocator, INT)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.of(allocator, INT, -1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).overlaps(allocator, EquatableValueSet.of(allocator, INT, -1).complement(allocator)));
    }

    @Test
    public void testContains()
            throws Exception
    {
        assertTrue(EquatableValueSet.all(allocator, INT).contains(allocator, EquatableValueSet.all(allocator, INT)));
        assertTrue(EquatableValueSet.all(allocator, INT).contains(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.all(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertTrue(EquatableValueSet.all(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertTrue(EquatableValueSet.all(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));

        assertFalse(EquatableValueSet.none(allocator, INT).contains(allocator, EquatableValueSet.all(allocator, INT)));
        assertTrue(EquatableValueSet.none(allocator, INT).contains(allocator, EquatableValueSet.none(allocator, INT)));
        assertFalse(EquatableValueSet.none(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertFalse(EquatableValueSet.none(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.none(allocator, INT).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));

        assertFalse(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.all(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.of(allocator, INT, 0).complement(allocator)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0).contains(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)));

        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.all(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.none(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 2)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 0).complement(allocator)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).contains(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)));

        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.all(allocator, INT)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.none(allocator, INT)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.of(allocator, INT, 0)));
        assertTrue(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.of(allocator, INT, -1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.of(allocator, INT, 0, 1)));
        assertFalse(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).contains(allocator, EquatableValueSet.of(allocator, INT, -1).complement(allocator)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        assertEquals(EquatableValueSet.none(allocator, INT).intersect(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.all(allocator, INT).intersect(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).intersect(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).intersect(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.all(allocator, INT).intersect(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).intersect(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).intersect(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).intersect(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).intersect(allocator, EquatableValueSet.of(allocator, INT, 1)), EquatableValueSet.of(allocator, INT, 1));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).intersect(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).intersect(allocator, EquatableValueSet.of(allocator, INT, 0, 2)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).intersect(allocator, EquatableValueSet.of(allocator, INT, 0, 2)), EquatableValueSet.of(allocator, INT, 2));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).intersect(allocator, EquatableValueSet.of(allocator, INT, 0, 2).complement(allocator)), EquatableValueSet.of(allocator, INT, 0, 1, 2).complement(allocator));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertEquals(EquatableValueSet.none(allocator, INT).union(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.all(allocator, INT).union(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).union(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).union(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.all(allocator, INT).union(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).union(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).union(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0, 1));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).union(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).union(allocator, EquatableValueSet.of(allocator, INT, 1)), EquatableValueSet.of(allocator, INT, 0).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).union(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 1).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).union(allocator, EquatableValueSet.of(allocator, INT, 0, 2)), EquatableValueSet.of(allocator, INT, 0, 1, 2));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).union(allocator, EquatableValueSet.of(allocator, INT, 0, 2)), EquatableValueSet.of(allocator, INT, 1).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator).union(allocator, EquatableValueSet.of(allocator, INT, 0, 2).complement(allocator)), EquatableValueSet.of(allocator, INT, 0).complement(allocator));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        assertEquals(EquatableValueSet.all(allocator, INT).subtract(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.all(allocator, INT).subtract(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.all(allocator, INT));
        assertEquals(EquatableValueSet.all(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0).complement(allocator));
        assertEquals(EquatableValueSet.all(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1)), EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator));
        assertEquals(EquatableValueSet.all(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 0, 1));

        assertEquals(EquatableValueSet.none(allocator, INT).subtract(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).subtract(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.none(allocator, INT).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)), EquatableValueSet.none(allocator, INT));

        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 0).complement(allocator)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 1)), EquatableValueSet.of(allocator, INT, 0));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 0));

        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.all(allocator, INT)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.none(allocator, INT)), EquatableValueSet.of(allocator, INT, 0).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 0)), EquatableValueSet.of(allocator, INT, 0).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 0).complement(allocator)), EquatableValueSet.none(allocator, INT));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 1)), EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 1));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1)), EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator));
        assertEquals(EquatableValueSet.of(allocator, INT, 0).complement(allocator).subtract(allocator, EquatableValueSet.of(allocator, INT, 0, 1).complement(allocator)), EquatableValueSet.of(allocator, INT, 1));
    }
}
