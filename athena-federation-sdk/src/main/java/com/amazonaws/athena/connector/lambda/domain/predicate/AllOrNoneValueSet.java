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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Describes a constraint as a ValueSet which can have one of several states:
 * 1. No value can match
 * 2. Only NULL values can match
 * 3. Only non-null values can match
 * 4. All values can match
 *
 * @see ValueSet
 */
public class AllOrNoneValueSet
        implements ValueSet
{
    private final ArrowType type;
    private final boolean all;
    private final boolean nullAllowed;

    /**
     * Constructs a new AllOrNoneValueSet.
     *
     * @param type The Apache Arrow type of the field that this ValueSet applies to.
     * @param all True if all non-null values are in this ValueSet.
     * @param nullAllowed True is all null values are in this ValueSet.
     */
    @JsonCreator
    public AllOrNoneValueSet(@JsonProperty("type") ArrowType type,
            @JsonProperty("all") boolean all,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
        this.nullAllowed = nullAllowed;
    }

    static AllOrNoneValueSet all(ArrowType type)
    {
        return new AllOrNoneValueSet(type, true, true);
    }

    static AllOrNoneValueSet none(ArrowType type)
    {
        return new AllOrNoneValueSet(type, false, false);
    }

    static AllOrNoneValueSet onlyNull(ArrowType type)
    {
        return new AllOrNoneValueSet(type, false, true);
    }

    static AllOrNoneValueSet notNull(ArrowType type)
    {
        return new AllOrNoneValueSet(type, true, false);
    }

    /**
     * Conveys if nulls should be allowed.
     *
     * @return True if NULLs satisfy this constraint, false otherwise.
     * @see ValueSet
     */
    @Override
    @JsonProperty("nullAllowed")
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
    @Override
    @JsonProperty
    public ArrowType getType()
    {
        return type;
    }

    /**
     * Conveys if no value can satisfy this ValueSet.
     *
     * @return True if no value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Override
    public boolean isNone()
    {
        return !all && !nullAllowed;
    }

    /**
     * Conveys if any value can satisfy this ValueSet.
     *
     * @return True if any value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Override
    @JsonProperty
    public boolean isAll()
    {
        return all && nullAllowed;
    }

    /**
     * Conveys if this ValueSet contains a single value.
     *
     * @return True if this ValueSet contains only a single value.
     * @note This is always false for AllOrNoneValueSet.
     */
    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    /**
     * Attempts to return the single value contained in this ValueSet.
     *
     * @return AllOrNoneValueSet never contains a single value, hence this method always throws UnsupportedOperationException.
     */
    @Override
    public Object getSingleValue()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Used to test if the supplied value (in the form of a Marker) is contained in this ValueSet.
     *
     * @param value The value to test in the form of a Marker.
     * @return True if the value is contained in the ValueSet, False otherwise.
     * @note This method is a basic building block of constraint evaluation.
     */
    @Override
    public boolean containsValue(Marker value)
    {
        if (value.isNullValue() && nullAllowed) {
            return true;
        }
        else if (value.isNullValue() && !nullAllowed) {
            return false;
        }

        return all;
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

        return all;
    }

    /**
     * @see ValueSet
     */
    @Override
    public ValueSet intersect(BlockAllocator allocator, ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all && otherValueSet.all, nullAllowed && other.isNullAllowed());
    }

    /**
     * @see ValueSet
     */
    @Override
    public ValueSet union(BlockAllocator allocator, ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all || otherValueSet.all, nullAllowed || other.isNullAllowed());
    }

    /**
     * @see ValueSet
     */
    @Override
    public ValueSet complement(BlockAllocator allocator)
    {
        return new AllOrNoneValueSet(type, !all, !nullAllowed);
    }

    @Override
    public String toString()
    {
        return "[" + (all ? "ALL" : "NONE") + " nullAllowed:" + isNullAllowed() + "]";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, all);
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
        final AllOrNoneValueSet other = (AllOrNoneValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.all == other.all;
    }

    private AllOrNoneValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched types: %s vs %s",
                    getType(), other.getType()));
        }
        if (!(other instanceof AllOrNoneValueSet)) {
            throw new IllegalArgumentException(String.format("ValueSet is not a AllOrNoneValueSet: %s",
                    other.getClass()));
        }
        return (AllOrNoneValueSet) other;
    }

    @Override
    public void close()
            throws Exception
    {
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s",
                    marker.getType(), getType()));
        }
    }
}
