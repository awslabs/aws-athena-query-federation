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

public class AllOrNoneValueSet
        implements ValueSet
{
    private final ArrowType type;
    private final boolean all;

    @JsonCreator
    public AllOrNoneValueSet(@JsonProperty("type") ArrowType type, @JsonProperty("all") boolean all)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
    }

    static AllOrNoneValueSet all(ArrowType type)
    {
        return new AllOrNoneValueSet(type, true);
    }

    static AllOrNoneValueSet none(ArrowType type)
    {
        return new AllOrNoneValueSet(type, false);
    }

    @Override
    @JsonProperty
    public ArrowType getType()
    {
        return type;
    }

    @Override
    public boolean isNone()
    {
        return !all;
    }

    @Override
    @JsonProperty
    public boolean isAll()
    {
        return all;
    }

    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    @Override
    public Object getSingleValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Marker value)
    {
        //TODO: Perhaps we need to do a type check here?
        return all;
    }

    @Override
    public ValueSet intersect(BlockAllocator allocator, ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all && otherValueSet.all);
    }

    @Override
    public ValueSet union(BlockAllocator allocator, ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all || otherValueSet.all);
    }

    @Override
    public ValueSet complement(BlockAllocator allocator)
    {
        return new AllOrNoneValueSet(type, !all);
    }

    @Override
    public String toString()
    {
        return "[" + (all ? "ALL" : "NONE") + "]";
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
}
