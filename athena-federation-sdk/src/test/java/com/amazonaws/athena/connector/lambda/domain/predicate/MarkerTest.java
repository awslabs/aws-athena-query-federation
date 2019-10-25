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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.arrow.vector.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class MarkerTest
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
    public void testTypes()
            throws Exception
    {
        assertEquals(Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()).getType(), Types.MinorType.INT.getType());
        assertEquals(Marker.below(allocator, Types.MinorType.INT.getType(), 1).getType(), Types.MinorType.INT.getType());
        assertEquals(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1).getType(), Types.MinorType.INT.getType());
        assertEquals(Marker.above(allocator, Types.MinorType.INT.getType(), 1).getType(), Types.MinorType.INT.getType());
        assertEquals(Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()).getType(), Types.MinorType.INT.getType());
    }

    @Test
    public void testUnbounded()
            throws Exception
    {
        assertTrue(Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()).isLowerUnbounded());
        assertFalse(Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()).isUpperUnbounded());
        assertTrue(Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()).isUpperUnbounded());
        assertFalse(Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()).isLowerUnbounded());

        assertFalse(Marker.below(allocator, Types.MinorType.INT.getType(), 1).isLowerUnbounded());
        assertFalse(Marker.below(allocator, Types.MinorType.INT.getType(), 1).isUpperUnbounded());
        assertFalse(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1).isLowerUnbounded());
        assertFalse(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1).isUpperUnbounded());
        assertFalse(Marker.above(allocator, Types.MinorType.INT.getType(), 1).isLowerUnbounded());
        assertFalse(Marker.above(allocator, Types.MinorType.INT.getType(), 1).isUpperUnbounded());
    }

    @Test
    public void testComparisons()
            throws Exception
    {
        ImmutableList<Marker> markers = ImmutableList.of(
                Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()),
                Marker.above(allocator, Types.MinorType.INT.getType(), 0),
                Marker.below(allocator, Types.MinorType.INT.getType(), 1),
                Marker.exactly(allocator, Types.MinorType.INT.getType(), 1),
                Marker.above(allocator, Types.MinorType.INT.getType(), 1),
                Marker.below(allocator, Types.MinorType.INT.getType(), 2),
                Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()));

        assertTrue(Ordering.natural().isStrictlyOrdered(markers));

        // Compare every marker with every other marker
        // Since the markers are strictly ordered, the value of the comparisons should be equivalent to the comparisons
        // of their indexes.
        for (int i = 0; i < markers.size(); i++) {
            for (int j = 0; j < markers.size(); j++) {
                assertTrue(markers.get(i).compareTo(markers.get(j)) == Integer.compare(i, j));
            }
        }
    }

    @Test
    public void testAdjacency()
            throws Exception
    {
        ImmutableMap<Marker, Integer> markers = ImmutableMap.<Marker, Integer>builder()
                .put(Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()), -1000)
                .put(Marker.above(allocator, Types.MinorType.INT.getType(), 0), -100)
                .put(Marker.below(allocator, Types.MinorType.INT.getType(), 1), -1)
                .put(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1), 0)
                .put(Marker.above(allocator, Types.MinorType.INT.getType(), 1), 1)
                .put(Marker.below(allocator, Types.MinorType.INT.getType(), 2), 100)
                .put(Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()), 1000)
                .build();

        // Compare every marker with every other marker
        // Map values of distance 1 indicate expected adjacency
        for (Map.Entry<Marker, Integer> entry1 : markers.entrySet()) {
            for (Map.Entry<Marker, Integer> entry2 : markers.entrySet()) {
                boolean adjacent = entry1.getKey().isAdjacent(entry2.getKey());
                boolean distanceIsOne = Math.abs(entry1.getValue() - entry2.getValue()) == 1;
                assertEquals(adjacent, distanceIsOne);
            }
        }

        assertEquals(Marker.below(allocator, Types.MinorType.INT.getType(), 1).greaterAdjacent(), Marker.exactly(allocator, Types.MinorType.INT.getType(), 1));
        assertEquals(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1).greaterAdjacent(), Marker.above(allocator, Types.MinorType.INT.getType(), 1));
        assertEquals(Marker.above(allocator, Types.MinorType.INT.getType(), 1).lesserAdjacent(), Marker.exactly(allocator, Types.MinorType.INT.getType(), 1));
        assertEquals(Marker.exactly(allocator, Types.MinorType.INT.getType(), 1).lesserAdjacent(), Marker.below(allocator, Types.MinorType.INT.getType(), 1));

        try {
            Marker.below(allocator, Types.MinorType.INT.getType(), 1).lesserAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.above(allocator, Types.MinorType.INT.getType(), 1).greaterAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()).lesserAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.lowerUnbounded(allocator, Types.MinorType.INT.getType()).greaterAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()).lesserAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.upperUnbounded(allocator, Types.MinorType.INT.getType()).greaterAdjacent();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }
}
