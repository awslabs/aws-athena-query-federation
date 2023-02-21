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
import java.util.Arrays;
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

/**
 * A set containing values that are represented as ranges that are sorted by their lower bound. For example:
 * col between 10 and 30, or col between 40 and 60, or col between 90 and 1000.
 *
 * @see ValueSet
 */
public class SortedRangeSet
        implements ValueSet
{
    private final boolean nullAllowed;
    private final ArrowType type;
    private final NavigableMap<ValueMarker, Range> lowIndexedRanges;

    private SortedRangeSet(ArrowType type, NavigableMap<ValueMarker, Range> lowIndexedRanges, boolean nullAllowed)
    {
        requireNonNull(type, "type is null");
        requireNonNull(lowIndexedRanges, "lowIndexedRanges is null");

        this.type = type;
        this.lowIndexedRanges = lowIndexedRanges;
        this.nullAllowed = nullAllowed;
    }

    public static SortedRangeSet none(ArrowType type)
    {
        return copyOf(type, Collections.emptyList(), false);
    }

    public static SortedRangeSet all(BlockAllocator allocator, ArrowType type)
    {
        return copyOf(type, Collections.singletonList(Range.all(allocator, type)), true);
    }

    public static SortedRangeSet onlyNull(ArrowType type)
    {
        return copyOf(type, Collections.emptyList(), true);
    }

    public static SortedRangeSet notNull(BlockAllocator allocator, ArrowType type)
    {
        return copyOf(type, Collections.singletonList(Range.all(allocator, type)), false);
    }

    static SortedRangeSet of(BlockAllocator allocator, ArrowType type, Object first, Object... rest)
    {
        return of(allocator, type, false, first, Arrays.asList(rest));
    }

    /**
     * Provided discrete values that are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet of(BlockAllocator allocator, ArrowType type, boolean nullAllowed, Object first, Collection<Object> rest)
    {
        List<Range> ranges = new ArrayList<>(rest.size() + 1);
        ranges.add(Range.equal(allocator, type, first));
        for (Object value : rest) {
            ranges.add(Range.equal(allocator, type, value));
        }
        return copyOf(type, ranges, nullAllowed);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet of(Range first, Range... rest)
    {
        return of(false, first, Arrays.asList(rest));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet of(boolean nullAllowed, Range first, Range... rest)
    {
        return of(nullAllowed, first, Arrays.asList(rest));
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    public static SortedRangeSet of(boolean nullAllowed, Range first, Collection<Range> rest)
    {
        List<Range> rangeList = new ArrayList<>(rest.size() + 1);
        rangeList.add(first);
        for (Range range : rest) {
            rangeList.add(range);
        }
        return copyOf(first.getType(), rangeList, nullAllowed);
    }

    /**
     * Provided Ranges are unioned together to form the SortedRangeSet
     */
    static SortedRangeSet copyOf(ArrowType type, Iterable<Range> ranges, boolean nullAllowed)
    {
        return new Builder(type, nullAllowed).addAll(ranges).build();
    }

    @JsonCreator
    public static SortedRangeSet copyOf(
            @JsonProperty("type") ArrowType type,
            @JsonProperty("ranges") List<Range> ranges,
            @JsonProperty("nullAllowed") boolean nullAllowed
    )
    {
        return copyOf(type, (Iterable<Range>) ranges, nullAllowed);
    }

    /**
     * Conveys if nulls should be allowed.
     *
     * @return True if NULLs satisfy this constraint, false otherwise.
     * @see ValueSet
     */
    @JsonProperty("nullAllowed")
    @Override
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    /**
     * The Arrow Type of the field this constraint applies to.
     *
     * @return The ArrowType of the field this ValueSet applies to.
     * @see ValueSet
     */
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

    /**
     * Conveys if no value can satisfy this ValueSet.
     *
     * @return True if no value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Transient
    @Override
    public boolean isNone()
    {
        return lowIndexedRanges.isEmpty();
    }

    /**
     * Conveys if any value can satisfy this ValueSet.
     *
     * @return True if any value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Transient
    @Override
    public boolean isAll()
    {
        return lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isAll();
    }

    /**
     * Conveys if this ValueSet contains a single value.
     *
     * @return True if this ValueSet contains only a single value.
     */
    @Transient
    @Override
    public boolean isSingleValue()
    {
        return (lowIndexedRanges.size() == 1 && lowIndexedRanges.values().iterator().next().isSingleValue() && !nullAllowed) ||
                lowIndexedRanges.isEmpty() && nullAllowed;
    }

    /**
     * Attempts to return the single value contained in this ValueSet.
     *
     * @return The single value contained in this ValueSet.
     * @throws IllegalStateException if this ValueSet does not contain exactly 1 value.
     */
    @Transient
    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("SortedRangeSet does not have just a single value");
        }

        if (nullAllowed && lowIndexedRanges.isEmpty()) {
            return null;
        }

        return lowIndexedRanges.values().iterator().next().getSingleValue();
    }

    /**
     * Used to test if the supplied value (in the form of a Marker) is contained in this ValueSet.
     *
     * @param marker The value to test in the form of a Marker.
     * @return True if the value is contained in the ValueSet, False otherwise.
     * @note This method is a basic building block of constraint evaluation.
     */
    @Override
    public boolean containsValue(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);

        if (marker.isNullValue() && nullAllowed) {
            return true;
        }
        else if (marker.isNullValue() && !nullAllowed) {
            return false;
        }

        if (marker.getBound() != Marker.Bound.EXACTLY) {
            throw new RuntimeException("Expected Bound.EXACTLY but found " + marker.getBound());
        }

        Map.Entry<ValueMarker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    /**
     * Used to test if the supplied value (in the form of a Marker) is contained in this ValueSet.
     *
     * @param value The value to test in the form of a Marker.
     * @return True if the value is contained in the ValueSet, False otherwise.
     * @note This method is a basic building block of constraint evaluation.
     */
    @Override
    public boolean containsValue(Object value)
    {
        if (value == null && nullAllowed) {
            return true;
        }
        else if (value == null && !nullAllowed) {
            return false;
        }
        LiteralValueMarker marker = new LiteralValueMarker(value, type);
        Map.Entry<ValueMarker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    boolean includesMarker(Marker marker)
    {
        requireNonNull(marker, "marker is null");
        checkTypeCompatibility(marker);

        if (marker.isNullValue() && nullAllowed) {
            return true;
        }

        Map.Entry<ValueMarker, Range> floorEntry = lowIndexedRanges.floorEntry(marker);
        return floorEntry != null && floorEntry.getValue().includes(marker);
    }

    /**
     * Gets a summary of the lowest lower bound and the highest upper bound that represents this ValueSet, keep in mind
     * that this summary may include more than the actual SortedRangeSet (e.g. col between 10 and 40 or col between 50 and 80
     * would yield a span of between 10 and 80).
     *
     * @return A Span which encompasses the lower bound and upper bound of the ValueSet.
     */
    @Transient
    public Range getSpan()
    {
        if (lowIndexedRanges.isEmpty()) {
            throw new IllegalStateException("Can not get span if no ranges exist");
        }
        return lowIndexedRanges.firstEntry().getValue().span(lowIndexedRanges.lastEntry().getValue());
    }

    /**
     * Provides access to the Ranges that comprise this ValueSet.
     *
     * @return The Ranges in the ValueSet.
     */
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

        boolean intersectNullAllowed = this.isNullAllowed() && other.isNullAllowed();
        Builder builder = new Builder(type, intersectNullAllowed);

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
        boolean unionNullAllowed = this.isNullAllowed() || other.isNullAllowed();
        SortedRangeSet otherRangeSet = checkCompatibility(other);
        return new Builder(type, unionNullAllowed)
                .addAll(this.getOrderedRanges())
                .addAll(otherRangeSet.getOrderedRanges())
                .build();
    }

    @Override
    public SortedRangeSet union(BlockAllocator allocator, Collection<ValueSet> valueSets)
    {
        boolean unionNullAllowed = this.isNullAllowed();
        for (ValueSet valueSet : valueSets) {
            unionNullAllowed |= valueSet.isNullAllowed();
        }

        Builder builder = new Builder(type, unionNullAllowed);
        builder.addAll(this.getOrderedRanges());
        for (ValueSet valueSet : valueSets) {
            builder.addAll(checkCompatibility(valueSet).getOrderedRanges());
        }
        return builder.build();
    }

    @Override
    public SortedRangeSet complement(BlockAllocator allocator)
    {
        Builder builder = new Builder(type, !nullAllowed);

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
        if (!getType().equals(marker.getType())
                && !checkTypeCompatibilityForTimeStamp(marker)) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s",
                    marker.getType(), getType()));
        }
    }

    /**
     * Since we are mapping MinorType.TIMESTAMPMILLITZ to ArrowType.Timestamp(MILLI, ZoneId.systemDefault().getId())
     * with UTC being a place holder,
     * We cannot check the type compatibility of such types, as UTC (place holder) can be/will be overwritten by
     * the TZ value coming from the raw data.
     *
     * Comparison can still be done for such types with different TZ as we convert to data to ZonedDateType to compare.
     *
     * @param marker
     * @return if both types are ArrowType.Timestamp, returns the equality of unit
     *         else false
     */
    private boolean checkTypeCompatibilityForTimeStamp(Marker marker)
    {
        return getType() instanceof ArrowType.Timestamp &&
                ((ArrowType.Timestamp) getType()).getUnit().equals(((ArrowType.Timestamp) marker.getType()).getUnit());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lowIndexedRanges, nullAllowed);
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
        if (this.nullAllowed != other.isNullAllowed()) {
            return false;
        }

        return Objects.equals(this.lowIndexedRanges, other.lowIndexedRanges);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("nullAllowed", nullAllowed)
                .add("lowIndexedRanges", lowIndexedRanges)
                .toString();
    }

    public static Builder newBuilder(ArrowType type, boolean nullAllowed)
    {
        return new Builder(type, nullAllowed);
    }

    public static class Builder
    {
        private final ArrowType type;
        private final boolean nullAllowed;
        private final List<Range> ranges = new ArrayList<>();

        Builder(ArrowType type, boolean nullAllowed)
        {
            requireNonNull(type, "type is null");
            this.type = type;
            this.nullAllowed = nullAllowed;
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

            NavigableMap<ValueMarker, Range> result = new TreeMap<>();

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

            return new SortedRangeSet(type, result, nullAllowed);
        }
    }

    @Override
    public void close()
            throws Exception
    {
        for (Map.Entry<ValueMarker, Range> next : lowIndexedRanges.entrySet()) {
            if (next.getKey() instanceof Marker) {
                ((Marker) next.getKey()).close();
            }
            next.getValue().close();
        }
    }
}
