/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import com.amazonaws.athena.connector.lambda.data.ArrowTypeComparator;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;

import static java.util.Objects.requireNonNull;

/**
 * A point on the continuous space defined by the specified type.
 * Each point may be just below, exact, or just above the specified value according to the Bound.
 * <p>
 * TODO: Add better support for SharedBlock Markers so that we have fewer Apache Arrow Blocks used to describe
 * constraints.
 */
public class Marker
        implements Comparable<ValueMarker>, AutoCloseable, ValueMarker
{
    protected static final String DEFAULT_COLUMN = "col1";

    public enum Bound
    {
        BELOW,   // lower than the value, but infinitesimally close to the value
        EXACTLY, // exactly the value
        ABOVE    // higher than the value, but infinitesimally close to the value
    }

    private final int valuePosition;
    private final Block valueBlock;
    private final Bound bound;
    private final boolean nullValue;
    private final ArrowType arrowType;
    private final Object value;

    /**
     * LOWER UNBOUNDED is specified with an empty value and a ABOVE bound
     * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
     */
    @JsonCreator
    public Marker(
            @JsonProperty("valueBlock") Block valueBlock,
            @JsonProperty("bound") Bound bound,
            @JsonProperty("nullValue") boolean nullValue)
    {
        requireNonNull(valueBlock, "valueBlock is null");
        requireNonNull(bound, "bound is null");

        this.valueBlock = valueBlock;
        this.bound = bound;
        this.nullValue = nullValue;
        this.valuePosition = 0;
        this.arrowType = valueBlock.getFieldReader(DEFAULT_COLUMN).getField().getType();
        if (!nullValue) {
            FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
            reader.setPosition(valuePosition);
            this.value = reader.readObject();
        }
        else {
            this.value = null;
        }
    }

    protected Marker(Block block,
            int valuePosition,
            Bound bound,
            boolean nullValue)
    {
        requireNonNull(block, "block is null");
        requireNonNull(bound, "bound is null");

        this.valueBlock = block;
        this.bound = bound;
        this.nullValue = nullValue;
        this.valuePosition = valuePosition;
        this.arrowType = valueBlock.getFieldReader(DEFAULT_COLUMN).getField().getType();
        if (!nullValue) {
            FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
            reader.setPosition(valuePosition);
            this.value = reader.readObject();
        }
        else {
            this.value = null;
        }
    }

    public boolean isNullValue()
    {
        return nullValue;
    }

    /**
     * The Arrow Type of the field this constraint applies to.
     *
     * @return The ArrowType of the field this ValueSet applies to.
     */
    @Transient
    public ArrowType getType()
    {
        return arrowType;
    }

    /**
     * Retrieves the value held in this Marker.
     *
     * @return The value.
     */
    @Transient
    public Object getValue()
    {
        if (nullValue) {
            throw new IllegalStateException("No value to get");
        }
        return value;
    }

    /**
     * Retrieves the Bound (BELOW, EXACTLY, ABOVE, etce...) used by this Marker.
     *
     * @return The Bound.
     */
    @JsonProperty
    public Bound getBound()
    {
        return bound;
    }

    /**
     * Provides access to the Apache Arrow Schema used to store the value of this marker.
     *
     * @return The Apache Arrow Schema used to store the value of this marker.
     * @note This is only used to avoid in serialization.
     */
    @Transient
    public Schema getSchema()
    {
        return valueBlock.getSchema();
    }

    /**
     * Provides access to the Apache Arrow Block used to store the value of this marker.
     *
     * @return The Apache Arrow Block used to store the value of this marker.
     * @note This is only used to avoid in serialization and will throw when called on a Marker that uses as SharedBlock.
     */
    @JsonProperty
    public Block getValueBlock()
    {
        if (valueBlock.getRowCount() > 1) {
            throw new RuntimeException("Attempting to get batch for a marker that appears to have a shared block");
        }
        return valueBlock;
    }

    @Transient
    public boolean isUpperUnbounded()
    {
        return nullValue && bound == Bound.BELOW;
    }

    @Transient
    public boolean isLowerUnbounded()
    {
        return nullValue && bound == Bound.ABOVE;
    }

    /**
     * Adjacency is defined by two Markers being infinitesimally close to each other.
     * This means they must share the same value and have adjacent Bounds.
     */
    @Transient
    public boolean isAdjacent(Marker other)
    {
        if (isUpperUnbounded() || isLowerUnbounded() || other.isUpperUnbounded() || other.isLowerUnbounded()) {
            return false;
        }

        if (ArrowTypeComparator.compare(getType(), getValue(), other.getValue()) != 0) {
            return false;
        }

        return (bound == Bound.EXACTLY && other.bound != Bound.EXACTLY) ||
                (bound != Bound.EXACTLY && other.bound == Bound.EXACTLY);
    }

    public Marker greaterAdjacent()
    {
        if (nullValue) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                return new Marker(valueBlock, valuePosition, Bound.EXACTLY, nullValue);
            case EXACTLY:
                return new Marker(valueBlock, valuePosition, Bound.ABOVE, nullValue);
            case ABOVE:
                throw new IllegalStateException("No greater marker adjacent to an ABOVE bound");
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public Marker lesserAdjacent()
    {
        if (nullValue) {
            throw new IllegalStateException("No marker adjacent to unbounded");
        }
        switch (bound) {
            case BELOW:
                throw new IllegalStateException("No lesser marker adjacent to a BELOW bound");
            case EXACTLY:
                return new Marker(valueBlock, valuePosition, Bound.BELOW, nullValue);
            case ABOVE:
                return new Marker(valueBlock, valuePosition, Bound.EXACTLY, nullValue);
            default:
                throw new AssertionError("Unsupported type: " + bound);
        }
    }

    public static Marker min(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) <= 0 ? marker1 : marker2;
    }

    public static Marker max(Marker marker1, Marker marker2)
    {
        return marker1.compareTo(marker2) >= 0 ? marker1 : marker2;
    }

    private static Marker create(BlockAllocator allocator, ArrowType type, Object value, Bound bound)
    {
        return new Marker(BlockUtils.newBlock(allocator, Marker.DEFAULT_COLUMN, type, value), 0, bound, false);
    }

    private static Marker create(BlockAllocator allocator, ArrowType type, Bound bound)
    {
        return new Marker(BlockUtils.newEmptyBlock(allocator, Marker.DEFAULT_COLUMN, type), 0, bound, true);
    }

    public static Marker upperUnbounded(BlockAllocator allocator, ArrowType type)
    {
        requireNonNull(type, "type is null");
        return create(allocator, type, Bound.BELOW);
    }

    public static Marker lowerUnbounded(BlockAllocator allocator, ArrowType type)
    {
        requireNonNull(type, "type is null");
        return create(allocator, type, Bound.ABOVE);
    }

    public static Marker above(BlockAllocator allocator, ArrowType type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(allocator, type, value, Bound.ABOVE);
    }

    public static Marker exactly(BlockAllocator allocator, ArrowType type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(allocator, type, value, Bound.EXACTLY);
    }

    public static Marker nullMarker(BlockAllocator allocator, ArrowType type)
    {
        return create(allocator, type, Bound.EXACTLY);
    }

    public static Marker below(BlockAllocator allocator, ArrowType type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");
        return create(allocator, type, value, Bound.BELOW);
    }

    @Override
    public int hashCode()
    {
        if (nullValue) {
            return com.google.common.base.Objects.hashCode(nullValue, getType(), bound);
        }

        return com.google.common.base.Objects.hashCode(nullValue, getType(), getValue(), bound);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Marker that = (Marker) o;

        boolean result = com.google.common.base.Objects.equal(nullValue, that.nullValue) &&
                com.google.common.base.Objects.equal(this.getType(), that.getType()) &&
                com.google.common.base.Objects.equal(this.bound, that.bound);

        if (result && !nullValue) {
            result = com.google.common.base.Objects.equal(this.getValue(), that.getValue());
        }

        return result;
    }

    @Override
    public int compareTo(ValueMarker o)
    {
        return ValueMarkerComparator.doCompare(this, o);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("valueBlock", getType())
                .add("nullValue", nullValue)
                .add("valueBlock", nullValue ? nullValue : getValue())
                .add("bound", bound)
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        valueBlock.close();
    }
}
