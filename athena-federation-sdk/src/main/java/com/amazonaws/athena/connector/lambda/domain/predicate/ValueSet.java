package com.amazonaws.athena.connector.lambda.domain.predicate;

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
