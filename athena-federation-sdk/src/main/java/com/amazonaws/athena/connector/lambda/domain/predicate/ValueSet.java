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
    ArrowType getType();

    @Transient
    boolean isNone();

    @Transient
    boolean isAll();

    @Transient
    boolean isSingleValue();

    @Transient
    Object getSingleValue();

    boolean containsValue(Marker value);

    /**
     * @return range predicates for orderable Types
     */
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
