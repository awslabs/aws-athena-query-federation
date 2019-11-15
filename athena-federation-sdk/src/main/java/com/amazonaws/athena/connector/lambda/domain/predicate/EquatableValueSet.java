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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included (aka whitelist)
 * or collectively excluded (aka !whitelist).
 */
public class EquatableValueSet
        implements ValueSet
{
    private static final String DEFAULT_COLUMN = "col1";
    private final boolean whiteList;
    private final Block valueBlock;
    public final boolean nullAllowed;

    @JsonCreator
    public EquatableValueSet(
            @JsonProperty("valueBlock") Block valueBlock,
            @JsonProperty("whiteList") boolean whiteList,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        requireNonNull(valueBlock, "valueBlock is null");
        this.valueBlock = valueBlock;
        this.whiteList = whiteList;
        this.nullAllowed = nullAllowed;
    }

    static EquatableValueSet none(BlockAllocator allocator, ArrowType type)
    {
        return new EquatableValueSet(BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, type), true, false);
    }

    static EquatableValueSet all(BlockAllocator allocator, ArrowType type)
    {
        return new EquatableValueSet(BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, type), false, true);
    }

    static EquatableValueSet onlyNull(BlockAllocator allocator, ArrowType type)
    {
        return new EquatableValueSet(BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, type), false, true);
    }

    static EquatableValueSet notNull(BlockAllocator allocator, ArrowType type)
    {
        return new EquatableValueSet(BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, type), false, false);
    }

    static EquatableValueSet of(BlockAllocator allocator, ArrowType type, Object... values)
    {
        return new EquatableValueSet(BlockUtils.newBlock(allocator, DEFAULT_COLUMN, type, values), true, false);
    }

    static EquatableValueSet of(BlockAllocator allocator, ArrowType type, boolean nullAllowed, Collection<Object> values)
    {
        return new EquatableValueSet(BlockUtils.newBlock(allocator, DEFAULT_COLUMN, type, values), true, nullAllowed);
    }

    @JsonProperty("nullAllowed")
    @Override
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    @Transient
    public Schema getSchema()
    {
        return valueBlock.getSchema();
    }

    @JsonProperty
    public Block getValueBlock()
    {
        return valueBlock;
    }

    @Transient
    @Override
    public ArrowType getType()
    {
        return valueBlock.getFieldReader(DEFAULT_COLUMN).getField().getType();
    }

    @JsonProperty
    public boolean isWhiteList()
    {
        return whiteList;
    }

    public Block getValues()
    {
        return valueBlock;
    }

    public Object getValue(int pos)
    {
        FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
        reader.setPosition(pos);
        return reader.readObject();
    }

    @Override
    public boolean isNone()
    {
        return whiteList && valueBlock.getRowCount() == 0 && !nullAllowed;
    }

    @Override
    public boolean isAll()
    {
        return !whiteList && valueBlock.getRowCount() == 0 && nullAllowed;
    }

    @Override
    public boolean isSingleValue()
    {
        return (whiteList && valueBlock.getRowCount() == 1 && !nullAllowed) ||
                (whiteList && valueBlock.getRowCount() == 0 && nullAllowed);
    }

    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("EquatableValueSet does not have just a single value");
        }

        if (nullAllowed && valueBlock.getRowCount() == 0) {
            return null;
        }

        FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
        reader.setPosition(0);
        return reader.readObject();
    }

    @Override
    public boolean containsValue(Marker marker)
    {
        if (marker.isNullValue() && nullAllowed) {
            return true;
        }
        else if (marker.isNullValue() && !nullAllowed) {
            return false;
        }

        Object value = marker.getValue();
        boolean result = false;
        FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
        for (int i = 0; i < valueBlock.getRowCount() && !result; i++) {
            reader.setPosition(i);
            result = ArrowTypeComparator.compare(reader, value, reader.readObject()) == 0;
        }
        return whiteList == result;
    }

    protected boolean containsValue(Object value)
    {
        if (value == null && nullAllowed) {
            return true;
        }

        boolean result = false;
        FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
        for (int i = 0; i < valueBlock.getRowCount() && !result; i++) {
            reader.setPosition(i);
            result = ArrowTypeComparator.compare(reader, value, reader.readObject()) == 0;
        }
        return whiteList == result;
    }

    @Override
    public EquatableValueSet intersect(BlockAllocator allocator, ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);
        boolean intersectNullAllowed = this.isNullAllowed() && other.isNullAllowed();

        if (whiteList && otherValueSet.isWhiteList()) {
            return new EquatableValueSet(intersect(allocator, this, otherValueSet), true, intersectNullAllowed);
        }
        else if (whiteList) {
            return new EquatableValueSet(subtract(allocator, this, otherValueSet), true, intersectNullAllowed);
        }
        else if (otherValueSet.isWhiteList()) {
            return new EquatableValueSet(subtract(allocator, otherValueSet, this), true, intersectNullAllowed);
        }
        else {
            return new EquatableValueSet(union(allocator, otherValueSet, this), false, intersectNullAllowed);
        }
    }

    @Override
    public EquatableValueSet union(BlockAllocator allocator, ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);
        boolean unionNullAllowed = this.isNullAllowed() || other.isNullAllowed();

        if (whiteList && otherValueSet.isWhiteList()) {
            return new EquatableValueSet(union(allocator, otherValueSet, this), true, unionNullAllowed);
        }
        else if (whiteList) {
            return new EquatableValueSet(subtract(allocator, otherValueSet, this), false, unionNullAllowed);
        }
        else if (otherValueSet.isWhiteList()) {
            return new EquatableValueSet(subtract(allocator, this, otherValueSet), false, unionNullAllowed);
        }
        else {
            return new EquatableValueSet(intersect(allocator, otherValueSet, this), false, unionNullAllowed);
        }
    }

    @Override
    public EquatableValueSet complement(BlockAllocator allocator)
    {
        return new EquatableValueSet(valueBlock, !whiteList, !nullAllowed);
    }

    @Override
    public String toString()
    {
        return "EquatableValueSet{" +
                "whiteList=" + whiteList +
                "nullAllowed=" + nullAllowed +
                ", valueBlock=" + valueBlock +
                '}';
    }

    private static Block intersect(BlockAllocator allocator, EquatableValueSet left, EquatableValueSet right)
    {
        Block resultBlock = BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, left.getType());
        FieldVector result = resultBlock.getFieldVector(DEFAULT_COLUMN);

        Block lhsBlock = left.getValues();

        FieldReader lhs = lhsBlock.getFieldReader(DEFAULT_COLUMN);

        int count = 0;
        for (int i = 0; i < lhsBlock.getRowCount(); i++) {
            lhs.setPosition(i);
            if (isPresent(lhs.readObject(), right.valueBlock)) {
                BlockUtils.setValue(result, count++, lhs.readObject());
            }
        }
        resultBlock.setRowCount(count);
        return resultBlock;
    }

    private static Block union(BlockAllocator allocator, EquatableValueSet left, EquatableValueSet right)
    {
        Block resultBlock = BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, left.getType());
        FieldVector result = resultBlock.getFieldVector(DEFAULT_COLUMN);

        Block lhsBlock = left.getValues();
        FieldReader lhs = lhsBlock.getFieldReader(DEFAULT_COLUMN);

        int count = 0;
        for (int i = 0; i < lhsBlock.getRowCount(); i++) {
            lhs.setPosition(i);
            BlockUtils.setValue(result, count++, lhs.readObject());
        }

        Block rhsBlock = right.getValues();
        FieldReader rhs = rhsBlock.getFieldReader(DEFAULT_COLUMN);
        for (int i = 0; i < rhsBlock.getRowCount(); i++) {
            rhs.setPosition(i);
            if (!isPresent(rhs.readObject(), left.valueBlock)) {
                BlockUtils.setValue(result, count++, rhs.readObject());
            }
        }

        resultBlock.setRowCount(count);
        return resultBlock;
    }

    private static Block subtract(BlockAllocator allocator, EquatableValueSet left, EquatableValueSet right)
    {
        Block resultBlock = BlockUtils.newEmptyBlock(allocator, DEFAULT_COLUMN, left.getType());
        FieldVector result = resultBlock.getFieldVector(DEFAULT_COLUMN);

        Block lhsBlock = left.getValues();

        FieldReader lhs = lhsBlock.getFieldReader(DEFAULT_COLUMN);

        int count = 0;
        for (int i = 0; i < lhsBlock.getRowCount(); i++) {
            lhs.setPosition(i);
            if (!isPresent(lhs.readObject(), right.valueBlock)) {
                BlockUtils.setValue(result, count++, lhs.readObject());
            }
        }
        resultBlock.setRowCount(count);
        return resultBlock;
    }

    private static boolean isPresent(Object lhs, Block right)
    {
        FieldReader rhs = right.getFieldReader(DEFAULT_COLUMN);
        Types.MinorType type = rhs.getMinorType();
        for (int j = 0; j < right.getRowCount(); j++) {
            rhs.setPosition(j);
            if (ArrowTypeComparator.compare(rhs, lhs, rhs.readObject()) == 0) {
                return true;
            }
        }
        return false;
    }

    private EquatableValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(String.format("Mismatched types: %s vs %s",
                    getType(), other.getType()));
        }
        if (!(other instanceof EquatableValueSet)) {
            throw new IllegalStateException(String.format("ValueSet is not a EquatableValueSet: %s", other.getClass()));
        }
        return (EquatableValueSet) other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getType(), whiteList, valueBlock, nullAllowed);
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
        final EquatableValueSet other = (EquatableValueSet) obj;

        if (this.getType() != null && other.getType() != null && Types.getMinorTypeForArrowType(this.getType()) == Types.getMinorTypeForArrowType(other.getType())) {
            //some arrow types require checking the minor type only, like Decimal.
            //We ignore any params though we may want to reconsider that in the future
        }
        else if (this.getType() != other.getType()) {
            return false;
        }

        if (this.whiteList != other.whiteList) {
            return false;
        }

        if (this.nullAllowed != other.nullAllowed) {
            return false;
        }

        if (this.valueBlock == null && other.valueBlock != null) {
            return false;
        }

        if (this.valueBlock != null && !this.valueBlock.equalsAsSet(other.valueBlock)) {
            return false;
        }

        return true;
    }

    @Override
    public void close()
            throws Exception
    {
        valueBlock.close();
    }

    public static Builder newBuilder(BlockAllocator allocator, ArrowType type, boolean isWhiteList, boolean nullAllowed)
    {
        return new Builder(allocator, type, isWhiteList, nullAllowed);
    }

    public static class Builder
    {
        private ArrowType type;
        private boolean isWhiteList;
        private boolean nullAllowed;
        private List<Object> values = new ArrayList<>();
        private BlockAllocator allocator;

        Builder(BlockAllocator allocator, ArrowType type, boolean isWhiteList, boolean nullAllowed)
        {
            requireNonNull(type, "minorType is null");
            this.allocator = allocator;
            this.type = type;
            this.isWhiteList = isWhiteList;
            this.nullAllowed = nullAllowed;
        }

        public Builder add(Object value)
        {
            values.add(value);
            return this;
        }

        public Builder addAll(Collection<Object> value)
        {
            values.addAll(value);
            return this;
        }

        public EquatableValueSet build()
        {
            return new EquatableValueSet(BlockUtils.newBlock(allocator, DEFAULT_COLUMN, type, values), isWhiteList, nullAllowed);
        }
    }

    private void checkTypeCompatibility(Marker marker)
    {
        if (!getType().equals(marker.getType())) {
            throw new IllegalStateException(String.format("Marker of %s does not match SortedRangeSet of %s",
                    marker.getType(), getType()));
        }
    }
}
