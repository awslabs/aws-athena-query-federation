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
 *
 * @see ValueSet
 */
public class EquatableValueSet
        implements ValueSet
{
    //The name of the single column used to represent values in the valueBlock.
    private static final String DEFAULT_COLUMN = "col1";
    private final boolean whiteList;
    private final Block valueBlock;
    public final boolean nullAllowed;

    /**
     * Constructs a new EquatableValueSet.
     *
     * @param valueBlock The values that are in this ValueSet expressed as a Block of Apache Arrow records.
     * @param whiteList True if this ValueSet is a white list (only these values), False if these are excluded values.
     * @param nullAllowed True if null values should be considered part of this ValueSet, False otherwise.
     */
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

    /**
     * Used to construct new Builder for EquatableValueSet.
     *
     * @param allocator The BlockAllocator to use when allocating Apache Arrow resources.
     * @param type The type of the field that this EquatableValueSet will apply to.
     * @param isWhiteList True if the EquatableValueSet will be a whitelist.
     * @param nullAllowed True if the EquatableValueSet should include NULL.
     * @return A new Builder that can be used to add values and create a new EquatableValueSet.
     */
    public static Builder newBuilder(BlockAllocator allocator, ArrowType type, boolean isWhiteList, boolean nullAllowed)
    {
        return new Builder(allocator, type, isWhiteList, nullAllowed);
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

    @Transient
    protected Schema getSchema()
    {
        return valueBlock.getSchema();
    }

    /**
     * Provides access to all the values in this ValueSet.
     *
     * @return The Block of Apache Arrow records in this ValueSet.
     */
    @JsonProperty
    public Block getValueBlock()
    {
        return valueBlock;
    }

    /**
     * The Arrow Type of the field this constraint applies to.
     *
     * @return The ArrowType of the field this ValueSet applies to.
     * @see ValueSet
     */
    @Transient
    @Override
    public ArrowType getType()
    {
        return valueBlock.getFieldReader(DEFAULT_COLUMN).getField().getType();
    }

    /**
     * Conveys if this ValueSet if a white list.
     *
     * @return True if the values in this ValueSet are part of a whitelist (e.g. list of values to include) vs a list of
     * * values to exclude.
     */
    @JsonProperty
    public boolean isWhiteList()
    {
        return whiteList;
    }

    /**
     * Provides access to all the values in this ValueSet.
     *
     * @return The Block of Apache Arrow records in this ValueSet.
     */
    public Block getValues()
    {
        return valueBlock;
    }

    /**
     * Retrieves the value at a specific position in this ValueSet.
     *
     * @param pos The position to retrieve, should be < size of ValueSet
     * @return The value at that position.
     */
    public Object getValue(int pos)
    {
        FieldReader reader = valueBlock.getFieldReader(DEFAULT_COLUMN);
        reader.setPosition(pos);
        return reader.readObject();
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
        return whiteList && valueBlock.getRowCount() == 0 && !nullAllowed;
    }

    /**
     * Conveys if any value can satisfy this ValueSet.
     *
     * @return True if any value can satisfy this ValueSet, false otherwise.
     * @see ValueSet
     */
    @Override
    public boolean isAll()
    {
        return !whiteList && valueBlock.getRowCount() == 0 && nullAllowed;
    }

    /**
     * Conveys if this ValueSet contains a single value.
     *
     * @return True if this ValueSet contains only a single value.
     */
    @Override
    public boolean isSingleValue()
    {
        return (whiteList && valueBlock.getRowCount() == 1 && !nullAllowed) ||
                (whiteList && valueBlock.getRowCount() == 0 && nullAllowed);
    }

    /**
     * Attempts to return the single value contained in this ValueSet.
     *
     * @return The single value contained in this ValueSet.
     * @throws IllegalStateException if this ValueSet does not contain exactly 1 value.
     */
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

    /**
     * Used to test if the supplied value  is contained in this ValueSet.
     *
     * @param value The value to test.
     * @return True if the value is contained in the ValueSet, False otherwise.
     * @note This method is a basic building block of constraint evaluation.
     */
    @Override
    public boolean containsValue(Object value)
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

    public static class Builder
    {
        private ArrowType type;
        private boolean isWhiteList;
        private boolean nullAllowed;
        private List<Object> values = new ArrayList<>();
        private BlockAllocator allocator;

        /**
         * Used to construct new Builder for EquatableValueSet.
         *
         * @param allocator The BlockAllocator to use when allocating Apache Arrow resources.
         * @param type The type of the field that this EquatableValueSet will apply to.
         * @param isWhiteList True if the EquatableValueSet will be a whitelist.
         * @param nullAllowed True if the EquatableValueSet should include NULL.
         */
        Builder(BlockAllocator allocator, ArrowType type, boolean isWhiteList, boolean nullAllowed)
        {
            requireNonNull(type, "minorType is null");
            this.allocator = allocator;
            this.type = type;
            this.isWhiteList = isWhiteList;
            this.nullAllowed = nullAllowed;
        }

        /**
         * Adds a value to the builder.
         *
         * @param value The value to add. Be sure that this matches the ArrowType.
         * @return The builder itself.
         */
        public Builder add(Object value)
        {
            values.add(value);
            return this;
        }

        /**
         * Adds the values to the builder.
         *
         * @param value Collection of values to add. Be sure that this matches the ArrowType.
         * @return The builder itself.
         */
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
