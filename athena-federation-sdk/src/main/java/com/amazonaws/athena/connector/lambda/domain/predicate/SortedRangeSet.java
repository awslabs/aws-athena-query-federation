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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public class SortedRangeSet
        implements ValueSet
{
    private final ArrowType type;
    private final NavigableMap<Marker, Range> lowIndexedRanges;

    private SortedRangeSet(ArrowType type, NavigableMap<Marker, Range> lowIndexedRanges)
    {
        requireNonNull(type, "type is null");
        requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");

        this.type = type;
        this.lowIndexedRanges = lowIndexedRanges;
    }

    static SortedRangeSet none(ArrowType type)
    {
        return copyOf(type, Collections.emptyList());
    }

    static SortedRangeSet all(BlockAllocator allocator, ArrowType type)
    {
        return copyOf(type, Collections.singletonList(Range.all(allocator, type)));
    }

    /**
     * Provided discrete values that are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(BlockAllocator allocator, ArrowType type, Object first, Object... rest)
    {
        List<Range> ranges = new ArrayList<>(rest.length + 1);
        ranges.add(Range.equal(allocator, type, first));
        for (Object value : rest) {
            ranges.add(Range.equal(allocator, type, value));
        }
        return copyOf(type, ranges);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet of(Range first, Range... rest)
    {
        List<Range> rangeList = new ArrayList<>(rest.length + 1);
        rangeList.add(first);
        for (Range range : rest) {
            rangeList.add(range);
        }
        return copyOf(first.getType(), rangeList);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet copyOf(ArrowType type, Iterable<Range> ranges)
    {
        return new Builder(type).addAll(ranges).build();
    }

    @JsonCreator
    public static SortedRangeSet copyOf(
            @JsonProperty("type") ArrowType type,
            @JsonProperty("ranges") List<Range> ranges)
    {
        return copyOf(type, (Iterable<Range>) ranges);
    }

    @JsonProperty
    public ArrowType getType()
    {
        return type;
    }

    @JsonProperty("ranges")
    public List<Range> getOrderedRanges()
    {
        return new ArrayList<>(lowIndexedRanges.values());
    }

    @Transient
    public int getRangeCount()
    {
        return lowIndexedRanges.size();
    }

    @Transient
    @Override
    public boolean isNone()
    {
        return lowIndexedRanges.isEmpty();
    }

    @Transient
    @Override
    public boolean isAll()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isAll();
    }

    @Transient
    @Override
    public boolean isSingleValue()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isSingleValue();
    }

    @Transient
    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("SortedRangeSet does not have just a single value");
        }
        return lowIndexedRanges.values().iterator().next().getSingleValue();
    }

    @Override
    public boolean containsValue(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);

        if (marker.getBound() != Marker.Bound.EXACTLY) {
            throw new RuntimeException("Expected Bound.EXACTLY but found " + marker.getBound());
        }

        Map.Entry<Marker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    boolean includesMarker(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);

        Map.Entry<Marker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    @Transient
    public Range getSpan()
    {
        if (lowIndexedRanges.isEmpty()) {
            throw new IllegalStateException("Can not get span if no ranges exist");
        }
        return lowIndexedRanges.firstEntry().getValue().span(lowIndexedRanges.lastEntry().getValue());
    }

    @Override
    public Ranges getRanges()
    {
        return new Ranges()
        {
            @Override
            public int getRangeCount()
            {
                return SortedRangeSet.this.getRangeCount();
            }

            @Override
            public List<Range> getOrderedRanges()
            {
                return SortedRangeSet.this.getOrderedRanges();
            }

            @Override
            public Range getSpan()
            {
                return SortedRangeSet.this.getSpan();
            }
        };
    }

    @Override
    public SortedRangeSet intersect(BlockAllocator allocator, ValueSet other)
    {
        SortedRangeSet otherRangeSet = checkCompatibility(other);

        Builder builder = new Builder(type);

        Iterator<Range> iterator1 = getOrderedRanges().iterator();
        Iterator<Range> iterator2 = otherRangeSet.getOrderedRanges().iterator();

        if (iterator1.hasNext() && iterator2.hasNext()) {
            Range range1 = iterator1.next();
            Range range2 = iterator2.next();

            while (true) {
                if (range1.overlaps(range2)) {
                    builder.add(range1.intersect(range2));
                }

                if (range1.getHigh().compareTo(range2.getHigh()) <= 0) {
                    if (!iterator1.hasNext()) {
                        break;
                    }
                    range1 = iterator1.next();
                }
                else {
                    if (!iterator2.hasNext()) {
                        break;
                    }
                    range2 = iterator2.next();
                }
            }
        }

        return builder.build();
    }

    @Override
    public SortedRangeSet union(BlockAllocator allocator, ValueSet other)
    {
        SortedRangeSet otherRangeSet = checkCompatibility(other);
        return new Builder(type)
                .addAll(this.getOrderedRanges())
                .addAll(otherRangeSet.getOrderedRanges())
                .build();
    }

    @Override
    public SortedRangeSet union(BlockAllocator allocator, Collection<ValueSet> valueSets)
    {
        Builder builder = new Builder(type);
        builder.addAll(this.getOrderedRanges());
        for (ValueSet valueSet : valueSets) {
            builder.addAll(checkCompatibility(valueSet).getOrderedRanges());
        }
        return builder.build();
    }

    @Override
    public SortedRangeSet complement(BlockAllocator allocator)
    {
        Builder builder = new Builder(type);

        if (lowIndexedRanges.isEmpty()) {
            return builder.add(Range.all(allocator, type)).build();
        }

        Iterator<Range> rangeIterator = lowIndexedRanges.values().iterator();

        Range firstRange = rangeIterator.next();
        if (!firstRange.getLow().isLowerUnbounded()) {
            builder.add(new Range(Marker.lowerUnbounded(allocator, type), firstRange.getLow().lesserAdjacent()));
        }

        Range previousRange = firstRange;
        while (rangeIterator.hasNext()) {
            Range currentRange = rangeIterator.next();

            Marker lowMarker = previousRange.getHigh().greaterAdjacent();
            Marker highMarker = currentRange.getLow().lesserAdjacent();
            builder.add(new Range(lowMarker, highMarker));

            previousRange = currentRange;
        }

        Range lastRange = previousRange;
        if (!lastRange.getHigh().isUpperUnbounded()) {
            builder.add(new Range(lastRange.getHigh().greaterAdjacent(), Marker.upperUnbounded(allocator, type)));
        }

        return builder.build();
    }

    private SortedRangeSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(String.format("Mismatched types: %s vs %s",
                    getType(), other.getType()));
        }
        if (!(other instanceof SortedRangeSet)) {
            throw new IllegalStateException(String.format("ValueSet is not a SortedRangeSet: %s", other.getClass()));
        }
        return (SortedRangeSet) other;
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s",
                    marker.getType(), getType()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SortedRangeSet other = (SortedRangeSet) obj;
        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("lowIndexedRanges", lowIndexedRanges)
                .toString();
    }

    public static Builder newBuilder(ArrowType type)
    {
        return new Builder(type);
    }

    public static class Builder
    {
        private final ArrowType type;
        private final List<Range> ranges = new ArrayList<>();

        Builder(ArrowType type)
        {
            requireNonNull(type, "type is null");
            this.type = type;
        }

        public Builder add(Range range)
        {
            if (!type.equals(range.getType())) {
                throw new IllegalArgumentException(String.format("Range type %s does not match builder type %s",
                        range.getType(), type));
            }

            ranges.add(range);
            return this;
        }

        public Builder addAll(Iterable<Range> arg)
        {
            for (Range range : arg) {
                add(range);
            }
            return this;
        }

        public SortedRangeSet build()
        {
            Collections.sort(ranges, Comparator.comparing(Range::getLow));

            NavigableMap<Marker, Range> result = new TreeMap<>();

            Range current = null;
            for (Range next : ranges) {
                if (current == null) {
                    current = next;
                    continue;
                }

                if (current.overlaps(next) || current.getHigh().isAdjacent(next.getLow())) {
                    current = current.span(next);
                }
                else {
                    result.put(current.getLow(), current);
                    current = next;
                }
            }

            if (current != null) {
                result.put(current.getLow(), current);
            }

            return new SortedRangeSet(type, result);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        for (Map.Entry<Marker, Range> next : lowIndexedRanges.entrySet()) {
            next.getKey().close();
            next.getValue().close();
        }
    }
}
