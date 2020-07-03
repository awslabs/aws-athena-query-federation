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
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Defines a range whose boundaries are defined by two Markers (e.g. low and high). This is helpful when you want
 * to express a constraint as column between X and Z.
 */
public class Range
        implements AutoCloseable
{
    private final Marker low;
    private final Marker high;

    @JsonCreator
    public Range(
            @JsonProperty("low") Marker low,
            @JsonProperty("high") Marker high)
    {
        requireNonNull(low, "low value is null");
        requireNonNull(high, "high value is null");
        if (!low.getType().equals(high.getType())) {
            throw new IllegalArgumentException(
                    String.format("Marker types do not match: %s vs %s", low.getType(), high.getType()));
        }
        if (low.getBound() == Marker.Bound.BELOW) {
            throw new IllegalArgumentException("low bound must be EXACTLY or ABOVE");
        }
        if (high.getBound() == Marker.Bound.ABOVE) {
            throw new IllegalArgumentException("high bound must be EXACTLY or BELOW");
        }
        if (low.compareTo(high) > 0) {
            throw new IllegalArgumentException("low must be less than or equal to high");
        }
        this.low = low;
        this.high = high;
    }

    public static Range all(BlockAllocator allocator, ArrowType type)
    {
        return new Range(Marker.lowerUnbounded(allocator, type), Marker.upperUnbounded(allocator, type));
    }

    public static Range greaterThan(BlockAllocator allocator, ArrowType type, Object low)
    {
        return new Range(Marker.above(allocator, type, low), Marker.upperUnbounded(allocator, type));
    }

    public static Range greaterThanOrEqual(BlockAllocator allocator, ArrowType type, Object low)
    {
        return new Range(Marker.exactly(allocator, type, low), Marker.upperUnbounded(allocator, type));
    }

    public static Range lessThan(BlockAllocator allocator, ArrowType type, Object high)
    {
        return new Range(Marker.lowerUnbounded(allocator, type), Marker.below(allocator, type, high));
    }

    public static Range lessThanOrEqual(BlockAllocator allocator, ArrowType type, Object high)
    {
        return new Range(Marker.lowerUnbounded(allocator, type), Marker.exactly(allocator, type, high));
    }

    public static Range equal(BlockAllocator allocator, ArrowType type, Object value)
    {
        return new Range(Marker.exactly(allocator, type, value), Marker.exactly(allocator, type, value));
    }

    public static Range range(BlockAllocator allocator, ArrowType type, Object low, boolean lowInclusive, Object high, boolean highInclusive)
    {
        Marker lowMarker = lowInclusive ? Marker.exactly(allocator, type, low) : Marker.above(allocator, type, low);
        Marker highMarker = highInclusive ? Marker.exactly(allocator, type, high) : Marker.below(allocator, type, high);
        return new Range(lowMarker, highMarker);
    }

    public ArrowType getType()
    {
        return low.getType();
    }

    @JsonProperty
    public Marker getLow()
    {
        return low;
    }

    @JsonProperty
    public Marker getHigh()
    {
        return high;
    }

    @Transient
    public boolean isSingleValue()
    {
        return low.getBound() == Marker.Bound.EXACTLY && low.equals(high);
    }

    @Transient
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Range does not have just a single value");
        }
        return low.getValue();
    }

    @Transient
    public boolean isAll()
    {
        return low.isLowerUnbounded() && high.isUpperUnbounded();
    }

    public boolean includes(ValueMarker marker)
    {
        requireNonNull(marker, "marker is null");
        return low.compareTo(marker) <= 0 && high.compareTo(marker) >= 0;
    }

    public boolean contains(Range other)
    {
        return this.getLow().compareTo(other.getLow()) <= 0 &&
                this.getHigh().compareTo(other.getHigh()) >= 0;
    }

    public Range span(Range other)
    {
        Marker lowMarker = Marker.min(low, other.getLow());
        Marker highMarker = Marker.max(high, other.getHigh());

        return new Range(lowMarker, highMarker);
    }

    public boolean overlaps(Range other)
    {
        return this.getLow().compareTo(other.getHigh()) <= 0 &&
                other.getLow().compareTo(this.getHigh()) <= 0;
    }

    public Range intersect(Range other)
    {
        if (!this.overlaps(other)) {
            throw new IllegalArgumentException("Cannot intersect non-overlapping ranges");
        }
        Marker lowMarker = Marker.max(low, other.getLow());
        Marker highMarker = Marker.min(high, other.getHigh());
        return new Range(lowMarker, highMarker);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high);
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
        final Range other = (Range) obj;
        return Objects.equals(this.low, other.low) &&
                Objects.equals(this.high, other.high);
    }

    @Override
    public String toString()
    {
        return com.google.common.base.MoreObjects.toStringHelper(this)
                .add("low", low)
                .add("high", high)
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        low.close();
        high.close();
    }
}
