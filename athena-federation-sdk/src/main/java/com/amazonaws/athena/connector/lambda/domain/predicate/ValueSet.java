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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.beans.Transient;
import java.util.Collection;

/**
 * Defines a construct that can be used to describe a set of values without containing all the individual
 * values themselves. For example, 1,2,3,4,5,6,7,8,9,10 could be described by having the literal Integer values
 * in a HashSet or you could describe them as the INT values between 1 and 10 inclusive.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EquatableValueSet.class, name = "equatable"),
        @JsonSubTypes.Type(value = SortedRangeSet.class, name = "sortable"),
        @JsonSubTypes.Type(value = AllOrNoneValueSet.class, name = "allOrNone"),
})
public interface ValueSet
        extends AutoCloseable
{
    /**
     * The Arrow Type of the values represented in this ValueSet.
     *
     * @return The ArrowType of the values represented in this ValueSet.
     * @see ValueSet
     */
    ArrowType getType();

    /**
     * Conveys if no value can satisfy this ValueSet.
     *
     * @return True if no value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Transient
    boolean isNone();

    /**
     * Conveys if any value can satisfy this ValueSet.
     *
     * @return True if any value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Transient
    boolean isAll();

    /**
     * Conveys if this ValueSet contains a single value.
     *
     * @return True if this ValueSet contains only a single value.
     * @note This is always false for AllOrNoneValueSet.
     */
    @Transient
    boolean isSingleValue();

    /**
     * Attempts to return the single value contained in this ValueSet.
     *
     * @return The single value, if there is one, in this ValueSet.
     */
    @Transient
    Object getSingleValue();

    /**
     * Conveys if nulls should be allowed.
     *
     * @return True if NULLs are part of this ValueSet, False otherwise.
     */
    boolean isNullAllowed();

    /**
     * Used to test if the supplied value (in the form of a Marker) is contained in this ValueSet.
     *
     * @param value The value to test in the form of a Marker.
     * @return True if the value is contained in the ValueSet, False otherwise.
     * @note This method is a basic building block of constraint evaluation.
     */
    boolean containsValue(Marker value);

    boolean containsValue(Object value);

    /**
     * @return range predicates for orderable Types
     */
    @Transient
    default Ranges getRanges()
    {
        throw new UnsupportedOperationException();
    }

    ValueSet intersect(BlockAllocator allocator, ValueSet other);

    ValueSet union(BlockAllocator allocator, ValueSet other);

    default ValueSet union(BlockAllocator allocator, Collection<ValueSet> valueSets)
    {
        ValueSet current = this;
        for (ValueSet valueSet : valueSets) {
            current = current.union(allocator, valueSet);
        }
        return current;
    }

    ValueSet complement(BlockAllocator allocator);

    default boolean overlaps(BlockAllocator allocator, ValueSet other)
    {
        return !this.intersect(allocator, other).isNone();
    }

    default ValueSet subtract(BlockAllocator allocator, ValueSet other)
    {
        return this.intersect(allocator, other.complement(allocator));
    }

    default boolean contains(BlockAllocator allocator, ValueSet other)
    {
        return this.union(allocator, other).equals(this);
    }
}
